'use strict';

const fp = require('lodash/fp');
const { capitalCase } = require('change-case');

module.exports = {
    run_group_by_count,
    run_group_by,
    run_filters,
}

const max_values = 256;

const selected_types = ['string', 'boolean', 'integer', 'biginteger', 'decimal', 'float'];

async function run_group_by_count(uid, groupBy, {filters, publicationState}) {

    const meta = strapi.db.metadata.get(uid);

    filters = update_filters(meta, filters, publicationState);

    groupBy = get_group_by_array(meta, groupBy);

    const qb = strapi.db.entityManager.createQueryBuilder(uid);

    const to = qb.init({filters, groupBy}).count();

    const result = await to.execute();

    if (result) return result.length;

    return null;
}

async function run_group_by(uid, groupBy, {filters, fields, populate, publicationState, limit, sort: orderBy, start: offset}) {

    const meta = strapi.db.metadata.get(uid);

    filters = update_filters(meta, filters, publicationState);

    const [ gb_sql, bindings ] = get_gb_sql(meta, filters, groupBy);

    const qb = strapi.db.entityManager.createQueryBuilder(uid);
    const to = qb.init({select: fields, filters: {id: {$in: '*'}}, populate, orderBy, offset, limit});
    const result = to.getKnexQuery().toSQL();

    const sql = result.sql.replace(/`t0`/g, '`t1`').replace('in (?)', `in ( ${gb_sql} )`);
    result.bindings.shift();
    bindings.push(...result.bindings);

    const raw_result = await strapi.db.connection.context.raw(sql, bindings);

    const items = [];
    for (const raw_item of raw_result[0]) {
        const item = {};
        for (const [key, value] of Object.entries(raw_item)) {
            item[meta.columnToAttribute[key]] = value;
        }
        items.push(item);
    }

    return items;
}

async function run_filters(uid, filters_config, params) {

    filters_config = filters_config ? fp.cloneDeep(filters_config) : get_filters_config(uid, params.fields);

    const queries = build_queries(uid, filters_config, params);

    const result = {};
    const promises = [];
    for (const { key, sql, bindings } of queries) {
        promises.push(run_sql(result, key, sql, bindings));
    }

    await Promise.all(promises);
    return normalize(result, filters_config);
}

function get_gb_sql(meta, filters, groupBy) {

    const qb = strapi.db.entityManager.createQueryBuilder(meta.uid);

    groupBy = get_group_by_array(meta, groupBy);

    const to = qb.init({select: ['id'], filters, groupBy});
    const result = to.getKnexQuery().toSQL();
    const sql = result.sql.replace('select `t0`.`id`', 'select min(`t0`.`id`) as id');

    return [ sql, result.bindings ];
}

function get_filters_config(uid, fields) {

    const meta = strapi.db.metadata.get(uid);

    const filters_config = [];
    for (const [ key, { type } ] of Object.entries(meta.attributes)) {
        if (fields && !fields.includes(key)) continue;
        if (key === 'id' || !selected_types.includes(type)) continue;
        if (['string', 'boolean'].includes(type)) {
            filters_config.push({key, type: 'list', title: capitalCase(key)});
        } else {
            filters_config.push({key, type: 'range', title: capitalCase(key)});
        }
    }

    return filters_config;
}

function normalize(result, filters_config) {

    const ranges_data = result.ranges[0];
    const total = Number(ranges_data.total);

    const list = [];

    for (const {key, type, key_props, ...rest} of filters_config) {

        const entry = {key, type, ...rest};

        if (type === 'range') {

            const count = Number(ranges_data[`count_${key}`]);
            if (isNaN(count) || count === 0) continue;

            const min = ranges_data[`min_${key}`];
            if (isNaN(min)) continue;

            const max = ranges_data[`max_${key}`];
            if (isNaN(max)) continue;

            entry.min = min;
            entry.max = max;
            entry.count = count;
            entry.full_set = (count === total);

        } else if (type === 'list') {
            
            if (!result[key]) continue;

            if (result[key].length === 0 || result[key].length > max_values) continue;

            if (key_props) {

                let sum = 0;
                const items = [];
                for (const key_prop of key_props) {
                    const item = result[key].find(x => x.key === key_prop.key);
                    if (!item) continue;
                    const count = Number(item.count)
                    sum += count;
                    items.push({...key_prop, count});
                }
                entry.items = items;

                entry.full_set = (sum === total);

            } else {

                entry.items = result[key];

                let sum = 0;
                for (const item of entry.items) {
                    if (item.key) {
                        if (item.key.toUpperCase() === item.key) item.label = item.key;
                        else item.label = capitalCase(item.key);
                    }
                    item.count = Number(item.count);
                    sum += item.count;
                }
                entry.full_set = (sum === total);
            }

        }

        list.push(entry);
    }

    return list;
};

async function run_sql(result, key, sql, bindings) {
    const raw_result = await strapi.db.connection.context.raw(sql, bindings);
    result[key] = raw_result[0];
}

function get_sql_template(uid, filters, publicationState) {

    const meta = strapi.db.metadata.get(uid);

    filters = update_filters(meta, filters, publicationState);

    const qb = strapi.db.entityManager.createQueryBuilder(uid);
    const { sql, bindings } = qb.init(
        {filters, select: ['tmp_select'], groupBy: ['tmp_group_by']}
    ).getKnexQuery().toSQL();

    const sql_template = sql.replace('`t0`.`tmp_select`', '{{select}}').
        replace(' group by `tmp_group_by`', '{{groupBy}}');

    return { sql_template, bindings };
}

function build_queries(uid, filters_config, params) {

    const { sql_template, bindings } = get_sql_template(uid, params.filters, params.publicationState);

    const queries = []; 

    let ranges_select = 'count(*) as total';

    const attributes = strapi.db.metadata.get(uid).attributes;

    for (const {key, type} of filters_config) {
        
        if (!key || !type) continue;
        const column_name = attributes[key].columnName;
        if (type === 'list') {
            const select = `\`t0\`.${column_name} as \`key\`, count(\`${column_name}\`) as \`count\``;
            const groupBy = ` group by \`${column_name}\``;
            let sql = sql_template.replace('{{select}}', select);
            sql = sql.replace('{{groupBy}}', groupBy);
            queries.push({key, sql, bindings});
            continue;
        }
        if (type === 'range') {
            ranges_select += `, max(\`${column_name}\`) as \`max_${key}\`, min(\`${column_name}\`) as \`min_${key}\`, count(\`${column_name}\`) as \`count_${key}\``;
            continue;
        }
        console.error(`build_queries, ${key} has an unknown type ${type}`);
    }

    let sql = sql_template.replace('{{groupBy}}', '');
    sql = sql.replace('{{select}}', ranges_select);
    queries.push({ key: 'ranges', sql, bindings});

    return queries;
}

function get_group_by_array(meta, groupBy) {

    if (!Array.isArray(groupBy)) groupBy = [ groupBy ];

    const items = [];
    for (const key of groupBy) {
        const attribute = meta.attributes[key];
        if (!attribute) items.push(key);
        else items.push(attribute.columnName);
    }

    return items;
}

function update_filters(meta, filters, publicationState) {

    if (!filters) filters = {};

    if (!publicationState || publicationState === 'live') {
        if (meta.attributes.publishedAt) {
            filters.publishedAt = {$notNull: true};
        }
    }

    return filters;
}