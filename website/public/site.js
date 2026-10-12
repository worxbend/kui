"use strict";
const $ = (selector) => document.querySelector(selector);
const themeButton = $(".theme-toggle");
function updateThemeLabel() {
  themeButton.setAttribute(
    "aria-label",
    `Switch to ${document.documentElement.dataset.theme === "dark" ? "light" : "dark"} theme`,
  );
}
updateThemeLabel();
themeButton.addEventListener("click", () => {
  const theme =
    document.documentElement.dataset.theme === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = theme;
  try {
    localStorage.setItem("kui-site-theme", theme);
  } catch {}
  updateThemeLabel();
});
const menu = $(".menu-toggle");
menu.addEventListener("click", () => {
  const open = menu.getAttribute("aria-expanded") !== "true";
  menu.setAttribute("aria-expanded", String(open));
  menu.setAttribute(
    "aria-label",
    open ? "Close navigation" : "Open navigation",
  );
  $("#primary-nav").classList.toggle("open", open);
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    menu.setAttribute("aria-expanded", "false");
    menu.setAttribute("aria-label", "Open navigation");
    $("#primary-nav").classList.remove("open");
  }
});
const toast = $(".toast");
let toastTimer;
function announce(message) {
  clearTimeout(toastTimer);
  toast.textContent = message;
  toast.classList.add("visible");
  toastTimer = setTimeout(() => toast.classList.remove("visible"), 3000);
}
for (const button of document.querySelectorAll(".copy-code"))
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(
        button.closest("pre").querySelector("code").textContent,
      );
      const old = button.innerHTML;
      button.textContent = "Copied";
      announce("Code copied to clipboard");
      setTimeout(() => (button.innerHTML = old), 2000);
    } catch {
      announce("Copy unavailable. Select the code and copy it manually.");
    }
  });
for (const dialog of document.querySelectorAll("dialog")) {
  dialog
    .querySelector("[data-close]")
    .addEventListener("click", () => dialog.close());
  dialog.addEventListener("click", (event) => {
    const rect = dialog.getBoundingClientRect();
    if (
      event.target === dialog &&
      (event.clientX < rect.left ||
        event.clientX > rect.right ||
        event.clientY < rect.top ||
        event.clientY > rect.bottom)
    )
      dialog.close();
  });
}
const imageDialog = $("#image-dialog");
function openImage(src, title) {
  const img = $("#enlarged-image");
  img.src = src;
  img.alt = title;
  $("#image-title").textContent = title;
  $("#image-original").href = src;
  imageDialog.showModal();
}
for (const button of document.querySelectorAll("[data-image]"))
  button.addEventListener("click", () =>
    openImage(button.dataset.image, button.dataset.title),
  );
for (const img of document.querySelectorAll(".prose img")) {
  const button = document.createElement("button");
  button.className = "zoom-image";
  button.setAttribute("aria-label", `Enlarge ${img.alt}`);
  img.replaceWith(button);
  button.append(img);
  button.addEventListener("click", () => openImage(img.src, img.alt));
}
for (const button of document.querySelectorAll("[data-category]"))
  button.addEventListener("click", () => {
    const category = button.dataset.category;
    let count = 0;
    for (const item of document.querySelectorAll("[data-gallery-category]")) {
      const show =
        category === "all" || item.dataset.galleryCategory === category;
      item.hidden = !show;
      if (show) count++;
    }
    for (const other of document.querySelectorAll("[data-category]"))
      other.setAttribute("aria-pressed", String(other === button));
    $("#gallery-count").textContent =
      `${count} ${count === 1 ? "screen" : "screens"}`;
  });
$("#guide-select")?.addEventListener("change", (event) =>
  location.assign(event.target.value),
);
const searchDialog = $("#search-dialog"),
  searchInput = $("#search-input"),
  results = $("#search-results"),
  status = $("#search-status");
let searchIndex, searchPromise;
async function loadIndex() {
  if (searchIndex) return searchIndex;
  if (!searchPromise)
    searchPromise = fetch(`${document.body.dataset.base}search.json`)
      .then((response) => {
        if (!response.ok) throw new Error("Search unavailable");
        return response.json();
      })
      .then((index) => (searchIndex = index))
      .catch((error) => {
        searchPromise = undefined;
        throw error;
      });
  return searchPromise;
}
function renderResults() {
  if (!searchIndex) return;
  const query = searchInput.value.trim().toLowerCase();
  const terms = query.split(/\s+/).filter(Boolean);
  const ranked = searchIndex
    .map((item) => ({
      ...item,
      score: terms.reduce(
        (score, term) =>
          score +
          (item.title.toLowerCase().includes(term) ? 8 : 0) +
          (item.description.toLowerCase().includes(term) ? 3 : 0),
        0,
      ),
    }))
    .filter(
      (item) =>
        !terms.length ||
        terms.every((term) =>
          `${item.title} ${item.description} ${item.text}`
            .toLowerCase()
            .includes(term),
        ),
    )
    .sort((a, b) => b.score - a.score);
  results.replaceChildren();
  const visible = ranked.slice(0, 12);
  status.textContent = query
    ? ranked.length
      ? `${ranked.length} matching guides${ranked.length > 12 ? ". Showing the first 12." : "."}`
      : "No guides found. Try “consumer”, “filter”, or “registry”."
    : "Start with a guide, or type to search the documentation.";
  for (const item of visible) {
    const link = document.createElement("a");
    link.className = "search-result";
    link.href = item.url;
    const group = document.createElement("small");
    group.textContent = item.group;
    const title = document.createElement("strong");
    title.textContent = item.title;
    const excerpt = document.createElement("span");
    excerpt.textContent = item.description;
    link.append(group, title, excerpt);
    results.append(link);
  }
}
async function openSearch() {
  if (!searchDialog.open) searchDialog.showModal();
  searchInput.focus();
  status.textContent = "Loading documentation index…";
  try {
    await loadIndex();
    renderResults();
  } catch {
    status.textContent =
      "Could not load search. Close and reopen to retry, or browse the documentation navigation.";
  }
}
for (const button of document.querySelectorAll(".search-trigger"))
  button.addEventListener("click", openSearch);
searchInput.addEventListener("input", renderResults);
searchInput.addEventListener("keydown", (event) => {
  if (event.key === "ArrowDown") {
    event.preventDefault();
    results.querySelector("a")?.focus();
  }
  if (event.key === "Enter") {
    const first = results.querySelector("a");
    if (first) location.assign(first.href);
  }
});
document.addEventListener("keydown", (event) => {
  const editing =
    ["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement.tagName) ||
    document.activeElement.isContentEditable;
  if (
    !editing &&
    !imageDialog.open &&
    (event.key === "/" ||
      ((event.ctrlKey || event.metaKey) && event.key === "k"))
  ) {
    event.preventDefault();
    openSearch();
  }
});
const headings = document.querySelectorAll(".prose h2[id]");
if (headings.length && "IntersectionObserver" in window) {
  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries)
        if (entry.isIntersecting) {
          for (const link of document.querySelectorAll(".toc nav a"))
            link.classList.toggle(
              "active",
              link.hash === `#${entry.target.id}`,
            );
        }
    },
    { rootMargin: "-100px 0px -65% 0px" },
  );
  headings.forEach((heading) => observer.observe(heading));
}
