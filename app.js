const fs = require("fs");

const cors = require("@koa/cors");

const Koa = require("koa");
const app = new Koa();
app.proxy = true;

const Router = require("koa-router");
const router = new Router();

const bodyParser = require("koa-bodyparser");

const Events = require("./events");
const axios = require("axios");

const WebSocket = require("ws");

const SubsFilename = "res/subs.json";
const WsSubsFilename = "res/ws_subs.json";

function saveJson(json, filename) {
  try {
    const data = JSON.stringify(json);
    fs.writeFileSync(filename, data);
  } catch (e) {
    console.error("Failed to save JSON:", filename, e);
  }
}

function loadJson(filename, ignore) {
  try {
    let rawData = fs.readFileSync(filename);
    return JSON.parse(rawData);
  } catch (e) {
    if (!ignore) {
      console.error("Failed to load JSON:", filename, e);
    }
  }
  return null;
}

const PostTimeout = 1000;
const PastEventsLimit = 1020000;
const PastEventsTrimTo = 10000;

const MaxEventsLimit = 1000;
const DefaultEventsLimit = 100;

(async () => {
  const eventsFetcher = await Events.init();
  console.log("Events initialized", eventsFetcher.lastBlockHeight);

  const pastEvents = await eventsFetcher.fetchLastNEvents(PastEventsTrimTo);
  pastEvents.reverse();

  let scheduleUpdate, processEvents;

  scheduleUpdate = (delay) =>
    setTimeout(async () => {
      const events = await eventsFetcher.fetchEvents();
      pastEvents.push(...events);
      if (pastEvents.length > PastEventsLimit) {
        pastEvents.splice(0, pastEvents.length - PastEventsTrimTo);
      }
      console.log(
        `Fetched ${events.length} events. Total ${pastEvents.length} events.`
      );
      await processEvents(events);
      scheduleUpdate(1000);
    }, delay);

  const subs = loadJson(SubsFilename, true) || {};

  const WS_PORT = process.env.WS_PORT || 7071;

  const wss = new WebSocket.Server({ port: WS_PORT });
  console.log("WebSocket server listening on http://localhost:%d/", WS_PORT);

  const wsClients = new Map();
  const wsSubs = new Map();

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
      return Object.keys(filter).every((key) =>
        recursiveFilter(filter[key], obj[key])
      );
    } else if (Array.isArray(filter) && Array.isArray(obj)) {
      return filter.every((value, index) => recursiveFilter(value, obj[index]));
    } else {
      return filter === obj;
    }
  };

  const getFilteredEvents = (events, filter) => {
    return events.filter((event) =>
      Array.isArray(filter)
        ? filter.some((f) => recursiveFilter(f, event))
        : isObject(filter)
        ? recursiveFilter(filter, event)
        : false
    );
  };

  processEvents = async (events) => {
    Object.values(subs).forEach((sub) => {
      const filteredEvents = getFilteredEvents(events, sub.filter);
      // console.log("Filtered events:", filteredEvents.length);
      if (filteredEvents.length > 0 && sub.url) {
        sub.totalPosts = (sub.totalPosts || 0) + 1;
        axios({
          method: "post",
          url: sub.url,
          data: {
            secret: sub.secret,
            events: filteredEvents,
          },
          timeout: PostTimeout,
        })
          .then(() => {
            sub.successPosts = (sub.successPosts || 0) + 1;
          })
          .catch(() => {
            sub.failedPosts = (sub.failedPosts || 0) + 1;
          });
      }
    });

    [...wsSubs.values()].forEach((sub) => {
      const filteredEvents = getFilteredEvents(events, sub.filter);
      if (filteredEvents.length > 0 && wsClients.has(sub.ws)) {
        try {
          sub.ws.send(
            JSON.stringify({
              secret: sub.secret,
              events: filteredEvents,
            })
          );
        } catch (e) {
          console.log("Failed to send events to ws", e);
        }
      }
    });
  };

  const saveWsSubs = () => {
    saveJson(
      [...wsSubs.values()].map(
        ({ xForwardedFor, remoteAddress, secret, filter }) => ({
          xForwardedFor,
          remoteAddress,
          secret,
          filter,
        })
      ),
      WsSubsFilename
    );
  };

  const getPastEvents = (filter, limit) => {
    const filteredEvents = getFilteredEvents(pastEvents, filter);
    limit = Math.min(
      Math.max(parseInt(limit) || DefaultEventsLimit, 0),
      Math.min(MaxEventsLimit, filteredEvents.length)
    );
    return filteredEvents.slice(filteredEvents.length - limit);
  };

  wss.on("connection", (ws, req) => {
    console.log("WS Connection open");
    ws.on('error', console.error);

    wsClients.set(ws, null);

    ws.on("close", () => {
      console.log("connection closed");
      wsClients.delete(ws);
      wsSubs.delete(ws);
      saveWsSubs();
    });

    ws.on("message", (messageAsString) => {
      try {
        const message = JSON.parse(messageAsString);
        if ("filter" in message && "secret" in message) {
          console.log("WS subscribed to events");
          wsSubs.set(ws, {
            ws,
            secret: message.secret,
            filter: message.filter,
            xForwardedFor: req.headers["x-forwarded-for"],
            remoteAddress: req.connection.remoteAddress,
          });
          saveWsSubs();
          if (message.fetch_past_events) {
            ws.send(
              JSON.stringify({
                secret: message.secret,
                events: getPastEvents(
                  message.filter,
                  message.fetch_past_events
                ),
                note: "past",
              })
            );
          }
        }
      } catch (e) {
        console.log("Bad message", e);
      }
    });
  });

  scheduleUpdate(1);

  // Save subs once a minute
  setInterval(() => {
    saveJson(subs, SubsFilename);
  }, 60000);

  router.post("/events", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.body;
      if ("filter" in body) {
        ctx.body = JSON.stringify(
          {
            events: getPastEvents(body.filter, body.limit),
          },
          null,
          2
        );
      } else {
        ctx.body = 'err: Required fields are "filter"';
      }
    } catch (e) {
      ctx.body = `err: ${e}`;
    }
  });

  router.post("/subscribe", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.body;
      if ("filter" in body && "url" in body && "secret" in body) {
        const secret = body.secret;
        if (secret in subs) {
          throw new Error(`Secret "${secret}" is already present`);
        }
        subs[secret] = {
          ip: ctx.request.ip,
          filter: body.filter,
          url: body.url,
          secret,
        };
        saveJson(subs, SubsFilename);
        ctx.body = JSON.stringify(
          {
            ok: true,
          },
          null,
          2
        );
      } else {
        ctx.body = 'err: Required fields are "filter", "url", "secret"';
      }
    } catch (e) {
      ctx.body = `err: ${e}`;
    }
  });

  router.post("/unsubscribe", (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.body;
      const secret = body.secret;
      if (secret in subs) {
        delete subs[secret];
        saveJson(subs, SubsFilename);
        ctx.body = JSON.stringify(
          {
            ok: true,
          },
          null,
          2
        );
      } else {
        ctx.body = 'err: No subscription found for "secret"';
      }
    } catch (e) {
      ctx.body = `err: ${e}`;
    }
  });

  app
    .use(async (ctx, next) => {
      console.log(ctx.method, ctx.path);
      await next();
    })
    .use(cors())
    .use(bodyParser())
    .use(router.routes())
    .use(router.allowedMethods());

  const PORT = process.env.PORT || 3000;
  app.listen(PORT);
  console.log("Listening on http://localhost:%d/", PORT);
})();
