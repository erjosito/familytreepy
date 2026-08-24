import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

const people = [
  { id: "ada", fullname: "Ada Lovelace", firstname: "Ada", lastname: "Lovelace", alias: "Enchantress of Numbers", birthdate: "1815-12-10", birthplace: "London", isAlive: false, deathdate: "1852-11-27", gender: "female" },
  { id: "charles", fullname: "Charles Babbage", firstname: "Charles", lastname: "Babbage", alias: "", birthdate: "1791-12-26", birthplace: "London", isAlive: false, deathdate: "1871-10-18", gender: "male" },
];

async function mockRepresentativeApi(page: Page) {
  await page.route("**/api/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname;

    if (path === "/api/graph") {
      await route.fulfill({
        json: {
          nodes: people.map((person) => ({ ...person, level: 0 })),
          edges: [],
        },
      });
    } else if (path === "/api/persons") {
      await route.fulfill({ json: people });
    } else if (path === "/api/persons/ada/notes") {
      await route.fulfill({ json: [{ text: "Pioneer of computing", author: "test@example.com", timestamp: "2026-01-01T12:00:00Z" }] });
    } else if (path.startsWith("/api/persons/")) {
      const id = path.split("/").at(-1);
      const person = people.find((candidate) => candidate.id === id);
      await route.fulfill({
        status: person ? 200 : 404,
        json: person ? { ...person, relationships: [], siblings: [] } : { detail: "Not found" },
      });
    } else if (path === "/api/config/storage") {
      await route.fulfill({ json: { sas_token: "" } });
    } else if (path === "/api/renderers") {
      await route.fulfill({ json: [{ name: "classic", description: "Classic family tree" }] });
    } else if (path === "/api/auth/users") {
      await route.fulfill({ json: [{ email: "admin@example.com", role: "admin" }] });
    } else if (path === "/api/history") {
      await route.fulfill({
        json: [{
          id: "revision-1",
          timestamp: "2026-01-01T12:00:00Z",
          actor: "admin@example.com",
          operation: "update",
          entity_type: "person",
          entity_id: "ada",
          before: null,
          after: { attributes: { firstname: "Ada", lastname: "Lovelace" } },
          metadata: {},
          expires_at: "2026-02-01T12:00:00Z",
          can_rollback: true,
        }],
      });
    } else {
      await route.fulfill({ status: 404, json: { detail: `Unhandled test endpoint: ${path}` } });
    }
  });
}

const routes = [
  { name: "/", url: "/", ready: async (page: Page) => expect(page.getByRole("application", { name: "Interactive family tree" })).toBeVisible() },
  { name: "/person/", url: "/person/?id=ada", ready: async (page: Page) => expect(page.getByRole("heading", { name: "Ada Lovelace" })).toBeVisible() },
  { name: "/image/", url: "/image/", ready: async (page: Page) => expect(page.locator("#image-center-person option[value=ada]")).toHaveCount(1) },
  { name: "/grid/", url: "/grid/", ready: async (page: Page) => expect(page.getByRole("link", { name: "Ada" })).toBeVisible() },
  { name: "/admin/", url: "/admin/", ready: async (page: Page) => expect(page.getByText("admin@example.com", { exact: true })).toBeVisible() },
];

for (const route of routes) {
  test(`${route.name} has no automatically detectable WCAG A/AA violations`, async ({ page }) => {
    await mockRepresentativeApi(page);
    await page.goto(route.url);
    await page.locator("#main-content").waitFor();
    await route.ready(page);

    const results = await new AxeBuilder({ page })
      .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"])
      .analyze();

    expect(results.violations).toEqual([]);
    if (page.viewportSize()?.width === 320) {
      const documentWidth = await page.evaluate(() => ({
        client: document.documentElement.clientWidth,
        scroll: document.documentElement.scrollWidth,
      }));
      expect(documentWidth.scroll).toBeLessThanOrEqual(documentWidth.client);
    }
  });
}

test("mobile navigation moves and restores keyboard focus", async ({ page }) => {
  await page.setViewportSize({ width: 320, height: 640 });
  await page.goto("/");

  const menuButton = page.getByRole("button", { name: "Open navigation menu" });
  await menuButton.click();
  await expect(page.getByRole("link", { name: "Explore", exact: true })).toBeFocused();

  await page.keyboard.press("Escape");
  await expect(menuButton).toBeFocused();
});

test("family graph supports keyboard exploration and actions", async ({ page }) => {
  await page.route("**/api/graph?**", async (route) => {
    await route.fulfill({
      json: {
        nodes: [
          { id: "ada", fullname: "Ada Lovelace", isAlive: false, level: 0 },
          { id: "charles", fullname: "Charles Babbage", isAlive: false, level: 0 },
        ],
        edges: [],
      },
    });
  });
  await page.route("**/api/persons", async (route) => {
    await route.fulfill({
      json: [
        { id: "ada", fullname: "Ada Lovelace" },
        { id: "charles", fullname: "Charles Babbage" },
      ],
    });
  });
  await page.route("**/api/config/storage", async (route) => {
    await route.fulfill({ json: { sas_token: "" } });
  });
  await page.route("**/api/persons/charles", async (route) => {
    await route.fulfill({
      json: { id: "charles", fullname: "Charles Babbage", firstname: "Charles", lastname: "Babbage" },
    });
  });

  await page.goto("/");
  const graph = page.getByRole("application", { name: "Interactive family tree" });
  await graph.focus();
  await expect(page.locator("#graph-keyboard-status")).toContainText("Ada Lovelace");

  await page.keyboard.press("ArrowRight");
  await expect(page.locator("#graph-keyboard-status")).toContainText("Charles Babbage");

  await page.keyboard.press("Enter");
  await expect(page.getByRole("heading", { name: "Charles Babbage" })).toBeVisible();

  await graph.focus();
  await page.keyboard.press("Shift+F10");
  await expect(page.getByRole("dialog")).toBeVisible();
});

test("graph keyboard selection recovers when graph data changes", async ({ page }) => {
  await page.route("**/api/graph?**", async (route) => {
    const centered = new URL(route.request().url()).searchParams.has("root_id");
    await route.fulfill({
      json: {
        nodes: centered
          ? [{ id: "ada", fullname: "Ada Lovelace", isAlive: false, level: 0 }]
          : [
              { id: "ada", fullname: "Ada Lovelace", isAlive: false, level: 0 },
              { id: "charles", fullname: "Charles Babbage", isAlive: false, level: 0 },
            ],
        edges: [],
      },
    });
  });
  await page.route("**/api/persons", async (route) => {
    await route.fulfill({ json: people });
  });
  await page.route("**/api/config/storage", async (route) => {
    await route.fulfill({ json: { sas_token: "" } });
  });
  await page.route("**/api/persons/ada", async (route) => {
    await route.fulfill({
      json: { ...people[0], relationships: [], siblings: [] },
    });
  });

  await page.goto("/");
  const graph = page.getByRole("application", { name: "Interactive family tree" });
  await graph.focus();
  await page.keyboard.press("ArrowRight");
  await expect(page.locator("#graph-keyboard-status")).toContainText("Charles Babbage");

  await page.keyboard.press("C");
  await expect(page.locator("#graph-keyboard-status")).toContainText("Ada Lovelace");
  await page.keyboard.press("Enter");
  await expect(page.getByRole("heading", { name: "Ada Lovelace" })).toBeVisible();
});

test("graph keyboard selection remains active after graph rebuild", async ({ page }) => {
  await page.route("**/api/graph?**", async (route) => {
    await route.fulfill({
      json: {
        nodes: [
          { id: "ada", fullname: "Ada Lovelace", isAlive: false, level: 0 },
          { id: "charles", fullname: "Charles Babbage", isAlive: false, level: 0 },
        ],
        edges: [],
      },
    });
  });
  await page.route("**/api/persons", async (route) => {
    await route.fulfill({ json: people });
  });
  await page.route("**/api/config/storage", async (route) => {
    await route.fulfill({ json: { sas_token: "" } });
  });

  await page.goto("/");
  const graph = page.getByRole("application", { name: "Interactive family tree" });
  await graph.focus();
  await page.keyboard.press("ArrowRight");
  await expect(page.locator("#graph-keyboard-status")).toContainText("Charles Babbage");

  await page.locator("#graph-keyboard-status").evaluate((element) => {
    element.textContent = "";
  });
  await page.keyboard.press("C");
  await expect(page.locator("#graph-keyboard-status")).toContainText("Charles Babbage");
});
