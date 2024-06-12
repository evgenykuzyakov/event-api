require("dotenv").config();
const fs = require("fs");
const { getFilteredRows, saveJson, loadJson } = require("./utils");

const cors = require("@koa/cors");

const Koa = require("koa");
const app = new Koa();
app.proxy = true;

const Router = require("koa-router");
const router = new Router();

const bodyParser = require("koa-bodyparser");

const Fetcher = require("./fetcher");

const WebSocket = require("ws");

const ResPath = process.env.RES_PATH || "res";
const WsSubsFilename = ResPath + "/ws_subs.json";

const PastRowsTrimTo = parseInt(process.env.HISTORY_LIMIT || "1000000");
const PastRowsLimit = Math.round(PastRowsTrimTo * 1.02);

const SaveLastBlock = process.env.SAVE_LAST_BLOCK === "true";
const LastBlockFilename = ResPath + "/last_block.json";

const MaxRowsLimit = 1000;
const DefaultRowsLimit = 100;

(async () => {
  if (!fs.existsSync(ResPath)) {
    fs.mkdirSync(ResPath);
  }

  const lastBlockHeight =
    (SaveLastBlock && loadJson(LastBlockFilename, true)) || undefined;

  const action = process.env.ACTION;
  let filter = {};
  try {
    filter = JSON.parse(process.env.FILTER ?? "{}");
  } catch (e) {
    console.error("Failed to parse filter", e);
  }

  const fetcher = await Fetcher.init(lastBlockHeight);
  console.log("Fetcher initialized", fetcher.lastBlockHeight);

  const fetchNext = async () => {
    try {
      const res = await fetcher.fetchNextBlock();
      const rows = action === "actions" ? res.actions : res.events;
      return rows;
    } catch (e) {
      console.error(e);
    }
    return [];
  };

  const pastRows = [];
  // Spawning fetch thread
  let fetchThread, processRows;
  fetchThread = async () => {
    while (true) {
      try {
        const rows = await fetchNext();
        const filteredRows = getFilteredRows(rows, filter);
        pastRows.push(...filteredRows);
        if (pastRows.length > PastRowsLimit) {
          pastRows.splice(0, pastRows.length - PastRowsTrimTo);
        }
        console.log(
          `Added ${filteredRows.length} out of ${rows.length} ${action}. Total ${pastRows.length} ${action}.`
        );
        if (SaveLastBlock) {
          saveJson(fetcher.lastBlockHeight, LastBlockFilename);
        }
        processRows(rows);
      } catch (e) {
        console.error(e);
      }
    }
  };

  // const subs = loadJson(SubsFilename, true) || {};

  const WS_PORT = process.env.WS_PORT || 7071;

  const wss = new WebSocket.Server({ port: WS_PORT });
  console.log("WebSocket server listening on http://localhost:%d/", WS_PORT);

  const wsClients = new Map();
  const wsSubs = new Map();

  // subs.push({
  //   "filter": [{
  //     "account_id": "nft.nearapps.near",
  //     "status": "SUCCESS",
  //     "standard": "nep171",
  //     "event": "nft_mint",
  //     "data_account_id": "bla.near",
  //   }],
  //     "url": "http://127.0.0.1:3000/event"
  // });

  const processRowsInternal = async (rows) => {
    [...wsSubs.values()].forEach((sub) => {
      const filteredEvents = getFilteredRows(rows, sub.filter);
      if (filteredEvents.length > 0 && wsClients.has(sub.ws)) {
        try {
          sub.ws.send(
            JSON.stringify({
              secret: sub.secret,
              [action]: filteredEvents,
            })
          );
        } catch (e) {
          console.log(`Failed to send ${action} to ws`, e);
        }
      }
    });
  };

  processRows = (rows) => {
    processRowsInternal(rows).catch((e) =>
      console.error("Process Rows failed", e)
    );
  };

  console.log("Starting fetch thread");
  fetchThread().catch((e) => console.error("Fetch thread failed", e));

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

  const getPastRows = (filter, limit) => {
    const filteredRows = getFilteredRows(pastRows, filter);
    limit = Math.min(
      Math.max(parseInt(limit) || DefaultRowsLimit, 0),
      Math.min(MaxRowsLimit, filteredRows.length)
    );
    return filteredRows.slice(filteredRows.length - limit);
  };

  wss.on("connection", (ws, req) => {
    console.log("WS Connection open");
    ws.on("error", console.error);

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
          console.log(`WS subscribed to ${action}`);
          wsSubs.set(ws, {
            ws,
            secret: message.secret,
            filter: message.filter,
            xForwardedFor: req.headers["x-forwarded-for"],
            remoteAddress: req.connection.remoteAddress,
          });
          saveWsSubs();
          if (message[`fetch_past_${action}`]) {
            ws.send(
              JSON.stringify({
                secret: message.secret,
                [action]: getPastRows(
                  message.filter,
                  message[`fetch_past_${action}`]
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

  // // Save subs once a minute
  // setInterval(() => {
  //   saveJson(subs, SubsFilename);
  // }, 60000);

  router.post(`/${action}`, (ctx) => {
    ctx.type = "application/json; charset=utf-8";
    try {
      const body = ctx.request.body;
      if ("filter" in body) {
        ctx.body = JSON.stringify(
          {
            [action]: getPastRows(body.filter, body.limit),
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

  // router.post("/subscribe", (ctx) => {
  //   ctx.type = "application/json; charset=utf-8";
  //   try {
  //     const body = ctx.request.body;
  //     if ("filter" in body && "url" in body && "secret" in body) {
  //       const secret = body.secret;
  //       if (secret in subs) {
  //         throw new Error(`Secret "${secret}" is already present`);
  //       }
  //       subs[secret] = {
  //         ip: ctx.request.ip,
  //         filter: body.filter,
  //         url: body.url,
  //         secret,
  //       };
  //       saveJson(subs, SubsFilename);
  //       ctx.body = JSON.stringify(
  //         {
  //           ok: true,
  //         },
  //         null,
  //         2
  //       );
  //     } else {
  //       ctx.body = 'err: Required fields are "filter", "url", "secret"';
  //     }
  //   } catch (e) {
  //     ctx.body = `err: ${e}`;
  //   }
  // });
  //
  // router.post("/unsubscribe", (ctx) => {
  //   ctx.type = "application/json; charset=utf-8";
  //   try {
  //     const body = ctx.request.body;
  //     const secret = body.secret;
  //     if (secret in subs) {
  //       delete subs[secret];
  //       saveJson(subs, SubsFilename);
  //       ctx.body = JSON.stringify(
  //         {
  //           ok: true,
  //         },
  //         null,
  //         2
  //       );
  //     } else {
  //       ctx.body = 'err: No subscription found for "secret"';
  //     }
  //   } catch (e) {
  //     ctx.body = `err: ${e}`;
  //   }
  // });

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
