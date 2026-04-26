const relicList = document.querySelector("#relic-list");
const missionList = document.querySelector("#mission-list");
const modList = document.querySelector("#mod-list");
const syndicateList = document.querySelector("#syndicate-list");
const relicSearch = document.querySelector("#relic-search");
const missionSearch = document.querySelector("#mission-search");
const modSearch = document.querySelector("#mod-search");
const syndicateSearch = document.querySelector("#syndicate-search");
const syndicateVendorFilter = document.querySelector("#syndicate-vendor-filter");
const modFilterEnemy = document.querySelector("#mod-filter-enemy");
const modFilterMission = document.querySelector("#mod-filter-mission");
const modFilterOther = document.querySelector("#mod-filter-other");
const vaultedToggle = document.querySelector("#vaulted-toggle");
const relicTemplate = document.querySelector("#relic-card-template");
const missionTemplate = document.querySelector("#mission-card-template");
const modTemplate = document.querySelector("#mod-card-template");

const CDN_BASE = "https://cdn.warframestat.us/img/";

const state = {
  relics: [],
  missions: [],
  mods: [],
  syndicateMods: [],
  view: "relics",
  modsLoaded: false,
  syndicateModsLoaded: false,
};

const viewButtons = document.querySelectorAll(".view-btn");
const panels = document.querySelectorAll(".panel");

function plat(value) {
  return value == null ? "-" : `${Number(value).toFixed(2)}p`;
}

function text(value) {
  return value == null || value === "" ? "-" : String(value);
}

function pct(value) {
  return value == null ? "-" : `${Number(value).toFixed(2)}%`;
}

function imageUrl(imageName) {
  return imageName ? `${CDN_BASE}${imageName}` : "data:image/gif;base64,R0lGODlhAQABAAAAACw=";
}

function emptyNode(message) {
  const div = document.createElement("div");
  div.className = "empty";
  div.textContent = message;
  return div;
}

function createEvGrid(evByState) {
  const fragment = document.createDocumentFragment();
  for (const [key, value] of Object.entries(evByState)) {
    const wrap = document.createElement("div");
    const dt = document.createElement("dt");
    dt.textContent = key;
    const dd = document.createElement("dd");
    dd.textContent = plat(value);
    wrap.append(dt, dd);
    fragment.append(wrap);
  }
  return fragment;
}

function rewardRow(reward) {
  const row = document.createElement("div");
  row.className = "detail-row";
  const rarityClass = `tag-${reward.rarity.toLowerCase()}`;
  const stateTags = Object.entries(reward.chances)
    .map(([name, chance]) => `<span class="tag tag-state">${name} ${pct(chance * 100)}</span>`)
    .join("");
  row.innerHTML = `
    <div class="detail-cell">
      <strong>${reward.item_name}</strong>
      <span class="tag ${rarityClass}">${reward.rarity}</span>
      <div class="tag-row">${stateTags}</div>
    </div>
    <div class="detail-cell muted">
      90d Median ${plat(reward.historical_median_90d ?? reward.median_sell_price)} · Listing Median ${plat(reward.median_sell_price)} · Listing Min ${plat(reward.min_sell_price)}
    </div>
  `;
  return row;
}

function missionRow(mission) {
  const row = document.createElement("div");
  row.className = "detail-row";
  row.innerHTML = `
    <div class="detail-cell">
      <strong>${mission.planet} / ${mission.node}</strong>
      <span class="muted">${text(mission.game_mode)} ${mission.rotation ? `· ${mission.rotation}` : ""}</span>
    </div>
    <div class="detail-cell muted">
      ${pct(mission.drop_chance)}
    </div>
  `;
  return row;
}

function sourceTag(category) {
  const tag = document.createElement("span");
  tag.className = `tag tag-source tag-${category}`;
  tag.textContent = category.replace(/_/g, " ");
  return tag;
}

function modSourceRow(source) {
  const row = document.createElement("div");
  row.className = "detail-row";

  const left = document.createElement("div");
  left.className = "detail-cell";
  const title = document.createElement("strong");
  title.textContent = source.source_name;
  const subtitle = document.createElement("span");
  subtitle.className = "muted";
  subtitle.textContent = source.source_detail || source.source_category;
  const tags = document.createElement("div");
  tags.className = "tag-row";
  tags.append(sourceTag(sourceGroupLabel(source)));
  if (source.rarity) {
    const rarity = document.createElement("span");
    rarity.className = `tag tag-${source.rarity.toLowerCase()}`;
    rarity.textContent = source.rarity;
    tags.append(rarity);
  }
  left.append(title, subtitle, tags);

  const right = document.createElement("div");
  right.className = "detail-cell muted";
  const details = [];
  if (source.chance != null) {
    details.push(`Chance ${pct(source.chance)}`);
  }
  if (source.enemy_mod_drop_chance != null) {
    details.push(`Enemy mod drop ${pct(source.enemy_mod_drop_chance)}`);
  }
  if (source.standing != null) {
    details.push(`Standing ${Number(source.standing).toLocaleString()}`);
  }
  right.textContent = details.length ? details.join(" · ") : "-";

  row.append(left, right);
  return row;
}

function groupModSources(sources) {
  const grouped = {
    enemy: [],
    mission: [],
    other: [],
  };
  for (const source of [...sources].sort(
    (a, b) => (b.chance ?? b.enemy_mod_drop_chance ?? -1) - (a.chance ?? a.enemy_mod_drop_chance ?? -1)
  )) {
    if (source.source_category === "enemy" || source.enemy_mod_drop_chance != null) {
      grouped.enemy.push(source);
    } else if (String(source.source_name || "").includes(", Rotation ")) {
      grouped.mission.push(source);
    } else {
      grouped.other.push(source);
    }
  }
  return grouped;
}

function renderModDetail(cardBody, data) {
  const price = data.price || {};
  const prices = cardBody.querySelector(".mod-prices");
  const sources = cardBody.querySelector(".mod-sources");
  prices.replaceChildren();
  sources.replaceChildren();

  prices.append(
    detailRow("90d Median", plat(price.historical_median_90d)),
    detailRow("Listing Median", plat(price.current_median_sell_price)),
    detailRow("Listing Avg", plat(price.avg_sell_price)),
    detailRow("Listing Min", plat(price.min_sell_price)),
    detailRow("Orders", price.order_count == null ? "-" : String(price.order_count))
  );

  const grouped = groupModSources(data.sources || []);
  if ((data.sources || []).length === 0) {
    sources.append(emptyNode("No mod sources cached yet."));
  } else {
    for (const [label, rows] of Object.entries(grouped)) {
      if (rows.length === 0) {
        continue;
      }
      const section = document.createElement("section");
      const header = document.createElement("h5");
      header.className = "subsection-title";
      header.textContent = label === "other" ? "other drop" : label;
      const list = document.createElement("div");
      list.className = "detail-table";
      for (const source of rows) {
        list.append(modSourceRow(source));
      }
      section.append(header, list);
      sources.append(section);
    }
  }
}

function detailRow(label, value) {
  const row = document.createElement("div");
  row.className = "detail-row";
  row.innerHTML = `
    <div class="detail-cell">
      <strong>${label}</strong>
    </div>
    <div class="detail-cell muted">${value}</div>
  `;
  return row;
}

function createModCard(mod) {
  const node = modTemplate.content.firstElementChild.cloneNode(true);
  const button = node.querySelector(".card-button");
  const detail = node.querySelector(".card-detail");
  const isSyndicate = Boolean(mod.vendor_names?.length);

  node.querySelector(".card-title").textContent = mod.name;
  node.querySelector(".pill").textContent = isSyndicate ? "Syndicate" : mod.is_augment ? "Augment" : text(mod.rarity);
  node.querySelector(".card-ev").textContent = `90d Median ${plat(mod.price?.historical_median_90d)} · Listing Median ${plat(mod.price?.current_median_sell_price)}`;
  node.querySelector(".card-meta").textContent =
    `${text(mod.mod_type)}${mod.polarity ? ` · ${mod.polarity}` : ""}${mod.base_drain != null ? ` · Drain ${mod.base_drain}` : ""}`;

  const tags = node.querySelector(".tag-row");
  const rarity = document.createElement("span");
  rarity.className = `tag tag-${String(mod.rarity || "common").toLowerCase()}`;
  rarity.textContent = text(mod.rarity);
  tags.append(rarity);
  if (mod.is_augment) {
    const augment = document.createElement("span");
    augment.className = "tag tag-augment";
    augment.textContent = "Augment";
    tags.append(augment);
  }
  if (isSyndicate) {
    const syndicate = document.createElement("span");
    syndicate.className = "tag tag-syndicate";
    syndicate.textContent = mod.vendor_names?.[0] || "Syndicate augment";
    tags.append(syndicate);
  }
  if (mod.is_tradable) {
    const tradable = document.createElement("span");
    tradable.className = "tag tag-tradable";
    tradable.textContent = "Tradable";
    tags.append(tradable);
  }

  let loaded = false;
  button.addEventListener("click", async () => {
    detail.classList.toggle("is-hidden");
    if (!loaded && !detail.classList.contains("is-hidden")) {
      loaded = true;
      await loadModDetail(detail, mod.mod_name);
    }
  });

  return node;
}

async function loadModDetail(cardBody, modName) {
  const response = await fetch(`/mods/${encodeURIComponent(modName)}`);
  if (!response.ok) {
    cardBody.querySelector(".mod-prices").replaceChildren(emptyNode("Mod details failed to load."));
    cardBody.querySelector(".mod-sources").replaceChildren();
    return;
  }
  const data = await response.json();
  renderModDetail(cardBody, data);
}

async function loadRelicDetail(cardBody, tier, relicName) {
  const response = await fetch(`/relics/${encodeURIComponent(tier)}/${encodeURIComponent(relicName)}`);
  const data = await response.json();

  const rewards = cardBody.querySelector(".rewards");
  rewards.replaceChildren();
  if (data.rewards.length === 0) {
    rewards.append(emptyNode("No reward pricing cached yet."));
  } else {
    for (const reward of data.rewards) {
      rewards.append(rewardRow(reward));
    }
  }

  const missions = cardBody.querySelector(".missions");
  missions.replaceChildren();
  if (data.missions.length === 0) {
    missions.append(emptyNode("No mission sources cached."));
  } else {
    for (const mission of data.missions.slice(0, 8)) {
      missions.append(missionRow(mission));
    }
  }
}

function createRelicCard(relic) {
  const node = relicTemplate.content.firstElementChild.cloneNode(true);
  const button = node.querySelector(".card-button");
  const detail = node.querySelector(".card-detail");
  const image = node.querySelector(".card-image");

  image.src = imageUrl(relic.image_name);
  image.alt = relic.name;
  node.querySelector(".card-title").textContent = relic.name;
  node.querySelector(".pill").textContent = relic.is_vaulted ? "Vaulted" : "Active";
  node.querySelector(".card-ev").textContent = `Primary EV ${plat(relic.ev)}`;
  node.querySelector(".ev-grid").append(createEvGrid(relic.ev_by_state));

  let loaded = false;
  button.addEventListener("click", async () => {
    detail.classList.toggle("is-hidden");
    if (!loaded && !detail.classList.contains("is-hidden")) {
      loaded = true;
      await loadRelicDetail(detail, relic.tier, relic.relic_name);
    }
  });

  return node;
}

function createMissionCard(mission) {
  const node = missionTemplate.content.firstElementChild.cloneNode(true);
  node.querySelector(".card-title").textContent = `${mission.planet} / ${mission.node}`;
  node.querySelector(".pill").textContent = mission.is_vaulted ? "Vaulted relic" : mission.tier;
  node.querySelector(".card-ev").textContent =
    `${plat(mission.expected_plat_per_reward)} per reward · ${mission.tier} ${mission.relic_name}`;
  node.querySelector(".card-meta").textContent =
    `${text(mission.game_mode)}${mission.rotation ? ` · ${mission.rotation}` : ""} · ${Number(mission.drop_chance).toFixed(2)}% relic chance`;
  return node;
}

function normalized(value) {
  return String(value || "").trim().toLowerCase();
}

function renderRelics() {
  const query = normalized(relicSearch.value);
  const includeVaulted = vaultedToggle.checked;
  const filtered = state.relics.filter((relic) => {
    if (!includeVaulted && relic.is_vaulted) {
      return false;
    }
    return normalized(relic.name).includes(query);
  });

  relicList.replaceChildren();
  if (filtered.length === 0) {
    relicList.append(emptyNode("No relics match the current filters."));
    return;
  }

  for (const relic of filtered) {
    relicList.append(createRelicCard(relic));
  }
}

function renderMissions() {
  const query = normalized(missionSearch.value);
  const includeVaulted = vaultedToggle.checked;
  const filtered = state.missions.filter((mission) => {
    if (!includeVaulted && mission.is_vaulted) {
      return false;
    }
    const haystack = normalized(
      `${mission.planet} ${mission.node} ${text(mission.game_mode)} ${mission.tier} ${mission.relic_name}`
    );
    return haystack.includes(query);
  });

  missionList.replaceChildren();
  if (filtered.length === 0) {
    missionList.append(emptyNode("No missions match the current filters."));
    return;
  }

  for (const mission of filtered) {
    missionList.append(createMissionCard(mission));
  }
}

function renderMods() {
  if (!state.modsLoaded) {
    modList.replaceChildren(emptyNode("Loading mods..."));
    return;
  }

  const query = normalized(modSearch.value);
  const enabledSourceFilters = {
    enemy: modFilterEnemy.checked,
    mission: modFilterMission.checked,
    other: modFilterOther.checked,
  };
  const filtered = state.mods.filter((mod) => {
    if (!normalized(mod.name).includes(query)) {
      return false;
    }
    if ((mod.source_count ?? 0) === 0) {
      return true;
    }
    return Object.entries(enabledSourceFilters).some(
      ([key, enabled]) => enabled && mod.source_flags?.[key]
    );
  });

  modList.replaceChildren();
  if (filtered.length === 0) {
    modList.append(emptyNode("No mods match the current filters."));
    return;
  }

  for (const mod of filtered) {
    modList.append(createModCard(mod));
  }
}

function renderSyndicateVendorOptions() {
  const selected = syndicateVendorFilter.value;
  const vendors = Array.from(new Set(state.syndicateMods.flatMap((mod) => mod.vendor_names || []))).sort();
  syndicateVendorFilter.replaceChildren();

  const allOption = document.createElement("option");
  allOption.value = "";
  allOption.textContent = "All syndicates";
  syndicateVendorFilter.append(allOption);

  for (const vendor of vendors) {
    const option = document.createElement("option");
    option.value = vendor;
    option.textContent = vendor;
    syndicateVendorFilter.append(option);
  }

  syndicateVendorFilter.value = vendors.includes(selected) ? selected : "";
}

function renderSyndicateMods() {
  if (!state.syndicateModsLoaded) {
    syndicateList.replaceChildren(emptyNode("Loading syndicate mods..."));
    return;
  }

  const query = normalized(syndicateSearch.value);
  const vendor = syndicateVendorFilter.value;
  const filtered = state.syndicateMods.filter((mod) => {
    if (vendor && !(mod.vendor_names || []).includes(vendor)) {
      return false;
    }
    const haystack = normalized(`${mod.name} ${(mod.vendor_names || []).join(" ")}`);
    return haystack.includes(query);
  });

  syndicateList.replaceChildren();
  if (filtered.length === 0) {
    syndicateList.append(emptyNode("No syndicate mods match the current filters."));
    return;
  }

  for (const mod of filtered) {
    syndicateList.append(createModCard(mod));
  }
}

function render() {
  renderRelics();
  renderMissions();
  renderMods();
  renderSyndicateMods();
}

function setView(view) {
  state.view = view;

  // update buttons
  viewButtons.forEach(btn => {
    btn.classList.toggle("is-active", btn.dataset.view === view);
  });

  // show/hide panels
  panels.forEach(panel => {
    panel.classList.toggle(
      "is-hidden",
      panel.dataset.view !== view
    );
  });

  if (view === "mods" && !state.modsLoaded) {
    modList.replaceChildren(emptyNode("Loading mods..."));
    loadMods().catch(console.error);
  }
  if (view === "syndicate" && !state.syndicateModsLoaded) {
    syndicateList.replaceChildren(emptyNode("Loading syndicate mods..."));
    loadSyndicateMods().catch(console.error);
  }
}

viewButtons.forEach(btn => {
  btn.addEventListener("click", () => {
    setView(btn.dataset.view);
  });
});

setView("relics");

async function loadDashboard() {
  const [relicResponse, missionResponse] = await Promise.all([
    fetch("/relics/top/vaulted?limit=0"),
    fetch("/missions/top/vaulted?limit=0"),
  ]);

  state.relics = await relicResponse.json();
  state.missions = await missionResponse.json();
  render();
}

async function loadMods() {
  const response = await fetch("/mods?limit=0");
  if (!response.ok) {
    modList.replaceChildren(emptyNode("Mods failed to load."));
    return;
  }
  state.mods = await response.json();
  if (state.mods.length === 0) {
    await fetch("/sync/mods", { method: "POST" });
    const retry = await fetch("/mods?limit=0");
    state.mods = await retry.json();
  }
  state.modsLoaded = true;
  renderMods();
}

async function loadSyndicateMods() {
  const response = await fetch("/syndicate-mods?limit=0");
  if (!response.ok) {
    syndicateList.replaceChildren(emptyNode("Syndicate mods failed to load."));
    syndicateVendorFilter.replaceChildren();
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "All syndicates";
    syndicateVendorFilter.append(option);
    return;
  }
  state.syndicateMods = await response.json();
  state.syndicateModsLoaded = true;
  renderSyndicateVendorOptions();
  renderSyndicateMods();
}

relicSearch.addEventListener("input", renderRelics);
missionSearch.addEventListener("input", renderMissions);
modSearch.addEventListener("input", renderMods);
modFilterEnemy.addEventListener("change", renderMods);
modFilterMission.addEventListener("change", renderMods);
modFilterOther.addEventListener("change", renderMods);
syndicateSearch.addEventListener("input", renderSyndicateMods);
syndicateVendorFilter.addEventListener("change", renderSyndicateMods);
vaultedToggle.addEventListener("change", render);

loadDashboard().catch(console.error);
