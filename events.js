require("dotenv").config();
const { createClient } = require('@clickhouse/client');

const MaxLimit = 10000;

const Events = {
  init: async function (lastBlockHeight) {
    this.client = createClient({
      host: process.env.DATABASE_URL,
      username: process.env.DATABASE_USER,
      password: process.env.DATABASE_PASSWORD,
      database: process.env.DATABASE_DATABASE,
    });
    if (!lastBlockHeight) {
      const query = await this.client.query({
        query: "SELECT MAX(block_height) last_block_height from events",
        format: 'JSONEachRow',
      });
      const res = await query.json();
      console.log(JSON.stringify(res, null, 2));
      lastBlockHeight = parseInt(res[0].last_block_height);
    }
    this.lastBlockHeight = lastBlockHeight;
    return this;
  },

  fetchLastNEvents: async function (limit) {
    const query = await this.client.query({
        query: "SELECT * from events order by block_height desc limit {limit: UInt32}",
        query_params: {limit},
        format: 'JSONEachRow',
      }
    );
    const res = await query.json();
    res.forEach((row) => {
      this.lastBlockHeight = Math.max(
        this.lastBlockHeight,
        parseInt(row.block_height)
      );
    });
    return res;
  },

  fetchEvents: async function () {
    const query = await this.client.query(
      {
        query: "SELECT * from events where block_height > {lastBlockHeight: UInt64} limit {limit: UInt32}",
        query_params: {limit: MaxLimit, lastBlockHeight: this.lastBlockHeight},
        format: 'JSONEachRow',
      }
    );
    const res = await query.json();
    res.forEach((row) => {
      this.lastBlockHeight = Math.max(
        this.lastBlockHeight,
        parseInt(row.block_height)
      );
    });
    return res;
  },
};

module.exports = Events;
