const productsContainer = document.getElementById("products-container");
const healthStatus = document.getElementById("health-status");
const btnRefresh = document.getElementById("btn-refresh");
const btnOrder30 = document.getElementById("btn-order-30");
const btnOrder60 = document.getElementById("btn-order-60");
const productsCount = document.getElementById("products-count");
const btnTrain = document.getElementById("btn-train");
const btnTests = document.getElementById("btn-tests");
const jobMeta = document.getElementById("job-meta");
const progressFill = document.getElementById("progress-fill");
const progressLabel = document.getElementById("progress-label");
const stepsList = document.getElementById("steps-list");
const jobLog = document.getElementById("job-log");

const orderModal = document.getElementById("order-modal");
const modalTitle = document.getElementById("modal-title");
const modalBody = document.getElementById("modal-body");
const modalCloseBtn = document.getElementById("modal-close-btn");
const modalFooterClose = document.getElementById("modal-footer-close");

let productsCache = [];
let activeJobId = null;
let pollTimer = null;
let productsLoaded = false;

async function fetchJson(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const text = await response.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { detail: text };
  }
  if (!response.ok) {
    const message = data.detail || data.message || response.statusText;
    throw new Error(typeof message === "string" ? message : JSON.stringify(message));
  }
  return data;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function setHealthPill(ok, label) {
  healthStatus.classList.toggle("ok", ok);
  healthStatus.classList.toggle("err", !ok);
  const labelEl = healthStatus.querySelector(".status-label");
  if (labelEl) labelEl.textContent = label;
}

function openModal() {
  orderModal.hidden = false;
  document.body.style.overflow = "hidden";
}

function closeModal() {
  orderModal.hidden = true;
  document.body.style.overflow = "";
}

/* —— tabs —— */
document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    const name = tab.dataset.tab;
    document.querySelectorAll(".tab").forEach((t) => {
      t.classList.toggle("active", t === tab);
      t.setAttribute("aria-selected", t === tab ? "true" : "false");
    });
    document.querySelectorAll(".tab-panel").forEach((panel) => {
      const active = panel.id === `tab-${name}`;
      panel.classList.toggle("active", active);
      panel.hidden = !active;
    });
    if (name === "orders" && !productsLoaded) {
      loadProducts();
      productsLoaded = true;
    }
  });
});

/* —— health —— */
async function loadHealth() {
  try {
    const data = await fetchJson("/api/health");
    const agents = data.agents || {};
    const labels = Object.entries(agents).map(([name, info]) => {
      const short = name.charAt(0).toUpperCase();
      return info.reachable ? `${short}✓` : `${short}✗`;
    });
    setHealthPill(
      data.all_healthy,
      data.all_healthy ? `Agenci OK (${labels.join(" ")})` : `Częściowo (${labels.join(" ")})`
    );
  } catch {
    setHealthPill(false, "Brak połączenia z API playground");
  }
}

/* —— jobs / progress —— */
function stepIcon(status) {
  switch (status) {
    case "pass":
      return "✓";
    case "fail":
      return "✗";
    case "running":
      return "…";
    case "skip":
      return "–";
    default:
      return "·";
  }
}

function renderJob(job) {
  const pct = Math.round((job.progress || 0) * 100);
  progressFill.style.width = `${pct}%`;
  progressLabel.textContent = `${pct}% — ${job.message || job.status}`;

  const kindLabel = job.kind === "train" ? "Trening" : "Testy";
  jobMeta.textContent = `${kindLabel} #${job.id} · ${job.status}${
    job.summary && Object.keys(job.summary).length
      ? ` · ${JSON.stringify(job.summary)}`
      : ""
  }`;

  stepsList.innerHTML = (job.steps || [])
    .map(
      (s) => `<li class="step-${escapeHtml(s.status)}">
        <span class="step-icon">${stepIcon(s.status)}</span>
        <span>${escapeHtml(s.label)}</span>
        ${s.detail ? `<span class="muted">(${escapeHtml(s.detail)})</span>` : ""}
      </li>`
    )
    .join("");

  const logText = (job.log || []).join("\n") || "Brak logów…";
  const nearBottom =
    jobLog.scrollHeight - jobLog.scrollTop - jobLog.clientHeight < 80;
  jobLog.textContent = logText;
  if (nearBottom) jobLog.scrollTop = jobLog.scrollHeight;

  const running = job.status === "running" || job.status === "queued";
  btnTrain.disabled = running;
  btnTests.disabled = running;
}

function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

async function pollJob() {
  if (!activeJobId) return;
  try {
    const job = await fetchJson(`/api/jobs/${activeJobId}`);
    renderJob(job);
    if (job.status === "succeeded" || job.status === "failed") {
      stopPolling();
      activeJobId = null;
      btnTrain.disabled = false;
      btnTests.disabled = false;
    }
  } catch (err) {
    jobMeta.textContent = `Błąd odczytu statusu: ${err.message}`;
    stopPolling();
    btnTrain.disabled = false;
    btnTests.disabled = false;
  }
}

async function startJob(endpoint) {
  stopPolling();
  btnTrain.disabled = true;
  btnTests.disabled = true;
  progressFill.style.width = "0%";
  progressLabel.textContent = "Start…";
  stepsList.innerHTML = "";
  jobLog.textContent = "Uruchamianie…";

  try {
    const job = await fetchJson(endpoint, { method: "POST", body: "{}" });
    activeJobId = job.id;
    renderJob(job);
    pollTimer = setInterval(pollJob, 800);
    pollJob();
  } catch (err) {
    jobMeta.textContent = err.message;
    jobLog.textContent = err.message;
    btnTrain.disabled = false;
    btnTests.disabled = false;
  }
}

btnTrain.addEventListener("click", () => startJob("/api/jobs/train"));
btnTests.addEventListener("click", () => startJob("/api/jobs/tests"));

/* —— orders tab —— */
function showModalLoading(horizonDays) {
  modalTitle.textContent = `Generowanie zamówień (${horizonDays} dni)…`;
  modalBody.innerHTML =
    '<div class="loading"><span class="spinner"></span> Prognoza popytu i kalkulacja zamówień…</div>';
  openModal();
}

function renderOrderModal(data) {
  const horizon = data.horizon_days;
  const summary = data.summary || {};
  const lines = data.lines || [];
  modalTitle.textContent = `Propozycja zamówienia — ${horizon} dni`;

  const sorted = [...lines].sort((a, b) => {
    const qtyA = Number(a.order_quantity) || 0;
    const qtyB = Number(b.order_quantity) || 0;
    if (qtyB !== qtyA) return qtyB - qtyA;
    return String(a.product_id).localeCompare(String(b.product_id));
  });

  const tableRows = sorted
    .map((line) => {
      if (line.error) {
        return `<tr class="row-error">
          <td><strong>${escapeHtml(line.product_id)}</strong></td>
          <td colspan="4">${escapeHtml(line.error)}</td>
        </tr>`;
      }
      const qty = Number(line.order_quantity) || 0;
      const qtyClass = qty > 0 ? "qty-highlight" : "qty-zero";
      return `<tr>
        <td><strong>${escapeHtml(line.product_id)}</strong></td>
        <td>${line.current_stock ?? "—"}</td>
        <td>${line.predicted_demand ?? "—"}</td>
        <td class="${qtyClass}">${qty}</td>
        <td>${line.projected_stock ?? "—"}</td>
      </tr>`;
    })
    .join("");

  modalBody.innerHTML = `
    <div class="modal-summary">
      <span>Produktów: <strong>${summary.products_checked ?? lines.length}</strong></span>
      <span>Do zamówienia: <strong>${summary.products_to_order ?? "—"}</strong></span>
      <span>Łącznie sztuk: <strong>${summary.total_units ?? "—"}</strong></span>
    </div>
    <div class="modal-table-wrap">
      <table>
        <thead>
          <tr>
            <th>Produkt</th>
            <th>Stan</th>
            <th>Prognoza popytu</th>
            <th>Do zamówienia</th>
            <th>Stan po zamówieniu</th>
          </tr>
        </thead>
        <tbody>${tableRows || '<tr><td colspan="5">Brak danych</td></tr>'}</tbody>
      </table>
    </div>`;
}

function renderProductsTable(products) {
  if (!products.length) {
    productsContainer.innerHTML =
      '<div class="empty">Brak produktów. Uruchom stack (setup.sh) i trening modeli.</div>';
    productsCount.textContent = "";
    return;
  }
  productsCount.textContent = `Monitorowanych produktów: ${products.length}`;
  const rows = products
    .map((p) => {
      const needsOrder = p.needs_order === true;
      const badge = needsOrder
        ? '<span class="badge badge-order">do zamówienia</span>'
        : '<span class="badge badge-ok">OK</span>';
      const policy = p.policy_code
        ? `<span class="badge badge-a">${escapeHtml(p.policy_code)}</span>`
        : "—";
      return `<tr>
        <td><strong>${escapeHtml(p.product_id)}</strong></td>
        <td>${p.current_stock ?? "—"}</td>
        <td>${p.forecast_demand_1m ?? "—"}</td>
        <td>${p.recommended_buy_qty ?? "—"}</td>
        <td>${policy}</td>
        <td>${badge}</td>
      </tr>`;
    })
    .join("");
  productsContainer.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Produkt</th><th>Stan</th><th>Prognoza (~1 mies.)</th>
          <th>Rekom. zakup</th><th>Polityka</th><th>Status</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
}

async function loadProducts() {
  productsContainer.innerHTML =
    '<div class="loading"><span class="spinner"></span> Ładowanie produktów…</div>';
  try {
    const data = await fetchJson("/api/products");
    productsCache = data.products || [];
    renderProductsTable(productsCache);
  } catch (err) {
    productsContainer.innerHTML = `<div class="error-box">${escapeHtml(err.message)}</div>`;
    productsCount.textContent = "";
  }
}

async function generateOrdersBatch(horizonDays) {
  if (!productsCache.length) {
    showModalLoading(horizonDays);
    modalBody.innerHTML =
      '<div class="error-box">Brak produktów na liście. Odśwież listę lub uruchom agentów.</div>';
    openModal();
    return;
  }
  [btnOrder30, btnOrder60].forEach((b) => {
    b.disabled = true;
  });
  showModalLoading(horizonDays);
  try {
    const data = await fetchJson("/api/orders-batch", {
      method: "POST",
      body: JSON.stringify({
        horizon_days: horizonDays,
        product_ids: productsCache.map((p) => p.product_id),
      }),
    });
    renderOrderModal(data);
  } catch (err) {
    modalTitle.textContent = "Błąd generowania zamówień";
    modalBody.innerHTML = `<div class="error-box">${escapeHtml(err.message)}</div>`;
  } finally {
    [btnOrder30, btnOrder60].forEach((b) => {
      b.disabled = false;
    });
  }
}

btnRefresh.addEventListener("click", () => {
  loadHealth();
  loadProducts();
});
btnOrder30.addEventListener("click", () => generateOrdersBatch(30));
btnOrder60.addEventListener("click", () => generateOrdersBatch(60));
modalCloseBtn.addEventListener("click", closeModal);
modalFooterClose.addEventListener("click", closeModal);
orderModal.querySelector("[data-close-modal]").addEventListener("click", closeModal);
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !orderModal.hidden) closeModal();
});

loadHealth();
