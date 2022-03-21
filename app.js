const Koa = require('koa');
const app = new Koa();

const Router = require('koa-router');
const router = new Router();

const bodyParser = require('koa-bodyparser');

const Events = require('./events');
const axios = require("axios");

const SubsFilename = "res/subs.json";

function saveJson(json, filename) {
  try {
    const data = JSON.stringify(json);
    fs.writeFileSync(filename, data);
  } catch (e) {
    console.error("Failed to save JSON:", filename, e);
  }
}

function loadJson(filename) {
  try {
    let rawData = fs.readFileSync(filename);
    return JSON.parse(rawData);
  } catch (e) {
    console.error("Failed to load JSON:", filename, e);
  }
  return null;
}

const PostTimeout = 1000;

(async () => {
  const eventsFetcher = await Events.init();

  let scheduleUpdate, processEvents;

  scheduleUpdate = (delay) => setTimeout(async () => {
    const events = await eventsFetcher.fetchEvents();
    console.log(`Fetched ${events.length} events`);
    await processEvents(events);
    scheduleUpdate(1000);
  }, delay);

  const subs = loadJson(SubsFilename) || {};

  // subs.push({
  //   "filter": [{
  //     "account_id": "nft.nearapps.near",
  //     "status": "SUCCESS",
  //     "event": {
  //       "standard": "nep171",
  //       "event": "nft_mint",
  //     }
  //   }],
  //     "url": "http://127.0.0.1:3000/event"
  // });

  const isObject = function (o) {
    return o === Object(o) && !Array.isArray(o) && typeof o !== "function";
  };

  const recursiveFilter = (filter, obj) => {
    if (isObject(filter) && isObject(obj)) {
      return Object.keys(filter).every((key) => recursiveFilter(filter[key], obj[key]));
    } else if (Array.isArray(filter) && Array.isArray(obj)) {
      return filter.every((value, index) => recursiveFilter(value, obj[index]));
    } else {
      return filter === obj;
    }
  }

  processEvents = async (events) => {
    Object.values(subs).forEach((sub) => {
      const filteredEvents = events.filter((event) => !sub.filter || sub.filter.some((f) => recursiveFilter(f, event)));
      // console.log("Filtered events:", filteredEvents.length);
      if (filteredEvents.length > 0 && sub.url) {
        axios({
          method: 'post',
          url: sub.url,
          data: {
            secret: sub.secret,
            events: filteredEvents
          },
          timeout: PostTimeout,
        }).catch(() => {})
      }
    })
  };

  scheduleUpdate(1);

  router.post('/subscribe', ctx => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const sub = ctx.request.body;
      if ("filter" in sub && "url" in sub && "secret" in sub) {
        const secret = sub.secret;
        if (secret in subs) {
          throw new Error(`Secret "${secret}" is already present`);
        }
        subs[secret] = sub;
        saveJson(subs, SubsFilename);
        ctx.body = JSON.stringify({
          "ok": true,
        }, null, 2);
      } else {
        ctx.body = 'err: Required fields are "filter", "url", "secret"'
      }
    } catch (e) {
      ctx.body = `err: ${e}`;
    }
  });

  router.post('/unsubscribe', ctx => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const req = ctx.request.body;
      const secret = req.secret;
      if (secret in subs) {
        delete subs[secret];
        saveJson(subs, SubsFilename);
        ctx.body = JSON.stringify({
          "ok": true,
        }, null, 2);
      } else {
        ctx.body = 'err: No subscription found for "secret"';
      }
    } catch (e) {
      ctx.body = `err: ${e}`;
    }
  });

  router.post('/event', ctx => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const events = ctx.request.body;
      console.log(events);
      ctx.body = "ok";
    } catch (e) {
      ctx.body = `err: ${e}`;
    }
  });


  app
    .use(async (ctx, next) => {
      console.log(ctx.method, ctx.path);
      await next();
    })
    .use(bodyParser())
    .use(router.routes())
    .use(router.allowedMethods());

  const PORT = process.env.PORT || 3000;
  app.listen(PORT);
  console.log('Listening on http://localhost:%d/', PORT);
})();
