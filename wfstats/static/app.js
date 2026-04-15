const relicList = document.querySelector("#relic-list");
const missionList = document.querySelector("#mission-list");
const relicSearch = document.querySelector("#relic-search");
const missionSearch = document.querySelector("#mission-search");
const vaultedToggle = document.querySelector("#vaulted-toggle");
const relicTemplate = document.querySelector("#relic-card-template");
const missionTemplate = document.querySelector("#mission-card-template");

const CDN_BASE = "https://cdn.warframestat.us/img/";

const state = {
  relics: [],
  missions: [],
};

function plat(value) {
  return value == null ? "-" : `${Number(value).toFixed(2)}p`;
}

function text(value) {
  return value == null || value === "" ? "-" : String(value);
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
  row.innerHTML = `
    <div class="detail-cell">
      <strong>${reward.item_name}</strong>
      <span class="muted">${reward.rarity}</span>
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
      ${Number(mission.drop_chance).toFixed(2)}%
    </div>
  `;
  return row;
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
  return value.trim().toLowerCase();
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

function render() {
  renderRelics();
  renderMissions();
}

async function loadDashboard() {
  const [relicResponse, missionResponse] = await Promise.all([
    fetch("/relics/top/vaulted?limit=0"),
    fetch("/missions/top/vaulted?limit=0"),
  ]);

  state.relics = await relicResponse.json();
  state.missions = await missionResponse.json();
  render();
}

relicSearch.addEventListener("input", renderRelics);
missionSearch.addEventListener("input", renderMissions);
vaultedToggle.addEventListener("change", render);

loadDashboard().catch(console.error);
