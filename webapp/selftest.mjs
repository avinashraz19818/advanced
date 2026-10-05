/**
 * Mini App builder ka self-test (node webapp/selftest.mjs)
 *
 * index.html + emoji-data.js ko minimal DOM stubs ke saath chalata hai aur check karta hai:
 *  - output bilkul advanced.py ke `rows_to_buttons_json()` jaisa hai (same keys/values)
 *  - premium emoji picker bot ke EMOJI_IDS se aata hai aur search (pizza -> 🍕) chalta hai
 *  - bot se aaya purana data (rows) theek load hota hai
 */
import fs from "node:fs";
import vm from "node:vm";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const html = fs.readFileSync(path.join(here, "index.html"), "utf8");
const emojiData = fs.existsSync(path.join(here, "emoji-data.js"))
  ? fs.readFileSync(path.join(here, "emoji-data.js"), "utf8") : "";
const script = html.match(/<script>([\s\S]*?)<\/script>\s*<\/body>/)[1];

/* ---------------- minimal DOM ---------------- */
function fakeEl(id = "") {
  return {
    id, value: "", textContent: "", innerHTML: "", placeholder: "", className: "",
    style: {}, dataset: {}, children: [],
    classList: { add(){}, remove(){}, toggle(){}, contains(){ return false; } },
    appendChild(c){ this.children.push(c); return c; },
    querySelectorAll(){ return []; }, querySelector(){ return null; }, closest(){ return null; },
    addEventListener(){}, remove(){},
  };
}
const els = new Map();
const document = {
  getElementById(id){ if (!els.has(id)) els.set(id, fakeEl(id)); return els.get(id); },
  createElement(){ return fakeEl(); },
  querySelectorAll(){ return []; },
  querySelector(){ return null; },
};
const store = new Map();
const sandbox = {
  document, console,
  window: {},                                  // browser/demo mode (Telegram WebApp nahi)
  location: { search: "" },
  localStorage: {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => store.set(k, String(v)),
  },
  navigator: {},
  alert(){}, prompt(){ return null; }, confirm(){ return true; },
  setTimeout, clearTimeout, JSON, URLSearchParams, atob, btoa, escape: globalThis.escape,
};
sandbox.window = sandbox;
sandbox.globalThis = sandbox;
vm.createContext(sandbox);
vm.runInContext(emojiData, sandbox);          // window.EMOJI_GROUPS
vm.runInContext(
  script + "\n;globalThis.__T = {buildJSON, importRows, state, cleanURL, EMOJI_MAP, EMOJI_GROUPS," +
  " emojiGroups, searchEmojis, pickEmoji, isPremium, pushRecent, loadRecent};",
  sandbox);
const T = sandbox.__T;

/* ---------------- checks ---------------- */
let pass = 0, fail = 0;
function check(name, cond, extra = "") {
  if (cond) { pass++; console.log("  ok   " + name); }
  else { fail++; console.log("  FAIL " + name + (extra ? "  -> " + extra : "")); }
}

// 1) URL normalisation
check("cleanURL: https waisa hi", T.cleanURL("https://t.me/x") === "https://t.me/x");
check("cleanURL: @username -> t.me link", T.cleanURL("@mychannel") === "https://t.me/mychannel");
check("cleanURL: bina scheme https lagta hai", T.cleanURL("t.me/x") === "https://t.me/x");

// 2) premium emoji map (bot ke EMOJI_IDS se) + picker groups
const emoCount = Object.keys(T.EMOJI_MAP).length;
check("emoji: premium map me 50+ emojis", emoCount > 50, String(emoCount));
check("emoji: premium id numeric", /^\d{10,}$/.test(T.EMOJI_MAP["💎"] || ""), T.EMOJI_MAP["💎"]);
check("emoji: isPremium sahi", T.isPremium("💎") === true && T.isPremium("🍕") === false);

// 2b) bot (advanced.py) aur picker ke premium emojis EXACT same hone chahiye
const advancedSrc = fs.readFileSync(path.join(here, "..", "advanced.py"), "utf8");
const botBlock = advancedSrc.match(/EMOJI_IDS = \{([\s\S]*?)\n\}/);
const botIds = {};
for (const m of (botBlock ? botBlock[1] : "").matchAll(/"([^"]+)":\s*"(\d+)"/g)) botIds[m[1]] = m[2];
const botCount = Object.keys(botIds).length;
const mismatches = [];
for (const [ch, id] of Object.entries(botIds)) if (T.EMOJI_MAP[ch] !== id) mismatches.push(`${ch}: bot=${id} picker=${T.EMOJI_MAP[ch]}`);
for (const ch of Object.keys(T.EMOJI_MAP)) if (!(ch in botIds)) mismatches.push(`${ch}: picker me extra`);
check(`emoji: bot ke ${botCount} premium emojis picker se exact match`, botCount > 50 && !mismatches.length,
      mismatches.slice(0, 4).join(" | "));

const groups = T.emojiGroups();
const names = groups.map(g => g.name);
check("picker: Premium tab sabse pehle", names[0] === "Premium", JSON.stringify(names.slice(0, 3)));
check("picker: Premium tab me bot ke emojis", groups[0].items.length === emoCount, String(groups[0].items.length));
check("picker: saari categories aati hain (9+)", groups.filter(g => !["Premium", "Recent"].includes(g.name)).length >= 9,
      JSON.stringify(names));

// 3) search: Telegram jaisa keyword search (pizza -> 🍕)
const pizza = T.searchEmojis("pizza").map(x => x[0]);
check("search: 'pizza' -> 🍕 milta hai", pizza.includes("🍕"), JSON.stringify(pizza.slice(0, 6)));
const fire = T.searchEmojis("fire").map(x => x[0]);
check("search: 'fire' -> 🔥 milta hai", fire.includes("🔥"), JSON.stringify(fire.slice(0, 6)));
const cake = T.searchEmojis("cake").map(x => x[0]);
check("search: 'cake' -> 🎂 milta hai", cake.includes("🎂"), JSON.stringify(cake.slice(0, 6)));
check("search: khaali search -> kuch nahi", T.searchEmojis("").length === 0);
check("search: anjaan shabd -> khaali", T.searchEmojis("zzzqqqxxx").length === 0);

// 4) emoji pick -> premium id ke saath set hota hai
T.pickEmoji("💎");
check("pick: premium emoji ka id set", T.state.pickEmoji.char === "💎" && T.state.pickEmoji.id === T.EMOJI_MAP["💎"],
      JSON.stringify(T.state.pickEmoji));
T.pickEmoji("🍕");
check("pick: normal emoji ka id khaali", T.state.pickEmoji.char === "🍕" && T.state.pickEmoji.id === "",
      JSON.stringify(T.state.pickEmoji));
check("pick: recent me chala gaya", T.loadRecent()[0] === "🍕", JSON.stringify(T.loadRecent().slice(0, 3)));
T.pickEmoji("");
check("pick: emoji hatao kaam karta hai", !T.state.pickEmoji.char, JSON.stringify(T.state.pickEmoji));

// 5) buildJSON == advanced.py rows_to_buttons_json (expected Python se generate kiya)
const EXPECTED = [
  [{ "text": "Register", "url": "https://t.me/Bot?start=reg", "icon_id": "5000000001", "icon_char": "🔗", "style": "success" }],
  [{ "text": "Channel", "url": "https://t.me/chan", "style": "primary" },
   { "text": "Support", "cb": "live_chat_support", "style": "primary" }],
  [{ "text": "Plain", "url": "https://t.me/x" }],
];
T.state.rows = [
  [{ text: "Register", url: "https://t.me/Bot?start=reg", cb: "", webapp: "", style: "success",
     icon_id: "5000000001", icon_char: "🔗" }],
  [{ text: "Channel", url: "https://t.me/chan", cb: "", webapp: "", style: "primary",
     icon_id: "", icon_char: "" },
   { text: "Support", url: "", cb: "live_chat_support", webapp: "", style: "primary",
     icon_id: "", icon_char: "" }],
  [{ text: "Plain", url: "https://t.me/x", cb: "", webapp: "", style: "",
     icon_id: "", icon_char: "" }],
];
const got = JSON.parse(JSON.stringify(T.buildJSON()));
check("buildJSON: python jaisa exact output", JSON.stringify(got) === JSON.stringify(EXPECTED),
      JSON.stringify(got));

// 6) style sirf valid values, khaali style omit
T.state.rows = [[{ text: "X", url: "https://t.me/x", cb: "", webapp: "", style: "rainbow",
                   icon_id: "", icon_char: "" }]];
check("buildJSON: galat style omit ho jata hai", !("style" in T.buildJSON()[0][0]),
      JSON.stringify(T.buildJSON()));

// 7) premium emoji button me id + char dono
T.state.rows = [[{ text: "Join", url: "@mychannel", cb: "", webapp: "", style: "success",
                   icon_char: "💎", icon_id: T.EMOJI_MAP["💎"] }]];
const emoBtn = T.buildJSON()[0][0];
check("buildJSON: premium emoji (id + char) save hota hai",
      emoBtn.icon_id === T.EMOJI_MAP["💎"] && emoBtn.icon_char === "💎" &&
      emoBtn.url === "https://t.me/mychannel", JSON.stringify(emoBtn));

// 8) bot se aaya purana data load
T.state.rows = [];
const n = T.importRows(EXPECTED);
check("import: bot ke rows load hote hain", n === 3 && T.state.rows.length === 3, String(n));
check("import: icon/style bache", T.state.rows[0][0].icon_char === "🔗" &&
      T.state.rows[0][0].style === "success", JSON.stringify(T.state.rows[0][0]));
check("import -> buildJSON round-trip",
      JSON.stringify(JSON.parse(JSON.stringify(T.buildJSON()))) === JSON.stringify(EXPECTED),
      JSON.stringify(T.buildJSON()));

// 9) client-facing UI (koi JSON/emoji-id nahi, premium look)
check("ui: koi JSON/emoji-id panel nahi", !/id="out"/.test(html) && !/fIconId/.test(html));
check("ui: live preview card hai", /id="pvKb"/.test(html) && /id="pvText"/.test(html));
check("ui: search + tabs + grid wala picker hai",
      /id="emojiSearch"/.test(html) && /id="emojiTabs"/.test(html) && /id="emojiGrid"/.test(html));
check("ui: search placeholder Telegram jaisa", /Search \(e\.g\. pizza/.test(html));
check("ui: premium badge (★) picker me", /class="pro"/.test(html));
check("ui: emoji-data.js load hota hai", /src="emoji-data\.js"/.test(html));

// Save regression: Telegram reply-keyboard launches legitimately have empty initData.
function saveHarness({platform = "android", launch = "keyboard", throws = false} = {}) {
  const sent = [];
  const env = {
    ...sandbox, window: null, TextEncoder,
    setTimeout(){ return 0; }, clearTimeout(){},
    location: {search: `?tid=test&launch=${launch}&text=100%25`},
    Telegram: {WebApp: {platform, initData: "", ready(){}, expand(){},
      sendData(data){ if (throws) throw Error("transport"); sent.push(JSON.parse(data)); }}},
  };
  env.window = env;
  env.globalThis = env;
  vm.createContext(env);
  vm.runInContext(emojiData, env);
  vm.runInContext(script + "\nglobalThis.saveTest = {save, state};", env);
  const api = env.saveTest;
  const isolated = api.state.rows.every(row => row.every(b => !b.url));
  api.state.rows = [[{text: "Join", url: "https://t.me/test", cb: "", style: "primary"}]];
  return {api, sent, isolated};
}
const live = saveHarness();
check("prefill: percent text decoded once", live.api.state.text === "100%");
check("prefill: target never loads unrelated browser draft", live.isolated);
live.api.save();
check("SAVE: empty initData still sends keyboard payload", live.sent.length === 1 &&
      live.sent[0].target.id === "test" && live.sent[0].rows[0][0].url === "https://t.me/test");
check("SAVE: UI does not falsely confirm database save", !document.getElementById("toast").textContent.includes("Save ho gaya"));
const browser = saveHarness({platform: "unknown"});
browser.api.save();
check("SAVE: ordinary browser cannot pretend to save", browser.sent.length === 0);
const inline = saveHarness({launch: "inline"});
inline.api.save();
check("SAVE: unsupported inline launch cannot silently lose data", inline.sent.length === 0);
const large = saveHarness();
large.api.state.text = "💎".repeat(1500);
large.api.save();
check("SAVE: 4096 byte limit checked before send", large.sent.length === 0);
const broken = saveHarness({throws: true});
broken.api.save();
check("SAVE: transport error shown without success", broken.sent.length === 0 &&
      document.getElementById("toast").textContent.includes("nahi paaye"));

// Direct inline mode: load via authenticated backend and close only after persisted ACK.
async function directHarness({failLoad = false, failSave = false} = {}){
  const calls = [], closes = [];
  const env = {
    ...sandbox, window: null, TextEncoder,
    location: {search: "?session=capability"},
    setTimeout(fn){ fn(); return 0; }, clearTimeout(){},
    Telegram: {WebApp: {platform: "android", initData: "signed-data", ready(){}, expand(){},
      close(){closes.push(true);}, sendData(){throw Error("Direct mode must not use sendData");}}},
    async fetch(path, options){
      calls.push({path, body: JSON.parse(options.body)});
      const fail = path === "/api/load" ? failLoad : failSave;
      return {ok: !fail, async json(){return fail ? {error: "DB unavailable"} :
        path === "/api/load" ? {ok:true, rows:[[{text:"Existing",url:"https://t.me/old"}]],text:"Original"} :
        {ok:true, saved:true};}};
    },
  };
  env.window = env; env.globalThis = env;
  vm.createContext(env); vm.runInContext(emojiData, env);
  vm.runInContext(script + "\nglobalThis.directTest = {save,state};", env);
  await new Promise(resolve => setImmediate(resolve));
  return {api:env.directTest,calls,closes};
}
const direct = await directHarness();
check("direct: existing buttons load for editing", direct.api.state.rows[0][0].text === "Existing");
await direct.api.save();
check("direct: SAVE uses same-origin API with Telegram authentication", direct.calls[1].path === "/api/save" &&
  direct.calls[1].body.initData === "signed-data" && direct.calls[1].body.session === "capability");
check("direct: closes after saved ACK", direct.closes.length === 1);
const failedSave = await directHarness({failSave:true});
await failedSave.api.save();
check("direct: failed SAVE keeps app open", failedSave.closes.length === 0);
const failedLoad = await directHarness({failLoad:true});
await failedLoad.api.save();
check("direct: failed load cannot overwrite existing buttons", failedLoad.calls.length === 1);

console.log(`\n==== webapp selftest: ${pass} passed, ${fail} failed ====`);
process.exit(fail ? 1 : 0);
