const {Client} = require('pg');

require('dotenv').config();

const MaxLimit = 10000;

const Events = {
  init: async function(lastBlockHeight) {
    const client = new Client({
      connectionString: process.env.DATABASE_URL,
    });
    await client.connect();
    this.client = client;
    if (!lastBlockHeight) {
      const res = await this.client.query('SELECT MAX(block_height) last_block_height from events');
      lastBlockHeight = parseInt(res.rows[0].last_block_height);
    }
    this.lastBlockHeight = lastBlockHeight;
    return this;
  },

  fetchEvents: async function() {
    const res = await this.client.query('SELECT * from events where block_height > $1 limit $2', [this.lastBlockHeight, MaxLimit]);
    res.rows.forEach((row) => {
      this.lastBlockHeight = Math.max(this.lastBlockHeight, parseInt(row.block_height));
      try {
        row.event = JSON.parse(row.event);
      } catch (e) {
        row.event = null;
      }
    })
    return res.rows;
  }
};

module.exports = Events;


