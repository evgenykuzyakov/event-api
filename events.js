require("dotenv").config();
const { createClient } = require('@clickhouse/client');

const MaxLimit = 10000;

const Events = {
  init: async function (lastBlockHeight) {
    this.table = process.env.DATABASE_TABLE;
    this.client = createClient({
      host: process.env.DATABASE_URL,
      username: process.env.DATABASE_USER,
      password: process.env.DATABASE_PASSWORD,
      database: process.env.DATABASE_DATABASE,
    });
    if (!lastBlockHeight) {
      const query = await this.client.query({
        query: `SELECT MAX(block_height) last_block_height from ${this.table}`,
        format: 'JSONEachRow',
      });
      const res = await query.json();
      lastBlockHeight = parseInt(res[0].last_block_height);
    }
    this.lastBlockHeight = lastBlockHeight;
    return this;
  },

  processEvents: function (events) {
    events.forEach((event) => {
      this.lastBlockHeight = Math.max(
        this.lastBlockHeight,
        parseInt(event.block_height)
      );
      event.block_timestamp = new Date(event.block_timestamp + "Z").getTime() / 1000;
    });
    return events;
  },

  fetchLastNEvents: async function (limit) {
    const query = await this.client.query({
        query: `SELECT * from ${this.table} order by block_height desc limit {limit: UInt32}`,
        query_params: {limit},
        format: 'JSONEachRow',
      }
    );
    return this.processEvents(await query.json());
  },

  fetchEvents: async function () {
    const query = await this.client.query(
      {
        query: `SELECT * from ${this.table} where block_height > {lastBlockHeight: UInt64} limit {limit: UInt32}`,
        query_params: {limit: MaxLimit, lastBlockHeight: this.lastBlockHeight},
        format: 'JSONEachRow',
      }
    );
    return this.processEvents(await query.json());
  },
};

module.exports = Events;
