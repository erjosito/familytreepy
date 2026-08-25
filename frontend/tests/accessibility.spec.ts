import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";
import { buildHierarchicalLayout } from "../src/components/GraphViewer";

const testPhoto = "data:image/gif;base64,R0lGODlhAQABAAAAACw=";

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
    } else if (path === "/api/persons/ada/pictures/people") {
      await new Promise((resolve) => setTimeout(resolve, 500));
      await route.fulfill({ json: [people[1]] });
    } else if (path.startsWith("/api/persons/")) {
      const id = path.split("/").at(-1);
      const person = people.find((candidate) => candidate.id === id);
      await route.fulfill({
        status: person ? 200 : 404,
        json: person ? {
          ...person,
          relationships: [],
          siblings: [],
          pictures: person.id === "ada" ? [testPhoto] : [],
        } : { detail: "Not found" },
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
        edges: [{
          id: "collaboration",
          source: "ada",
          target: "charles",
          type: "isSpouseOf",
        }],
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
  await expect(page.locator("#graph-keyboard-status")).toContainText("Charles Babbage");

  await page.keyboard.press("ArrowRight");
  await expect(page.locator("#graph-keyboard-status")).toContainText("Charles Babbage");

  await page.keyboard.press("Enter");
  await expect(page.getByRole("heading", { name: "Charles Babbage" })).toBeVisible();

  await graph.focus();
  await page.keyboard.press("Shift+F10");
  await expect(page.getByRole("dialog")).toBeVisible();
});

test("photo viewer exposes tags and restores focus when closed", async ({ page }) => {
  await mockRepresentativeApi(page);
  await page.goto("/person/?id=ada");
  await expect(page.getByRole("heading", { name: "Ada Lovelace" })).toBeVisible();

  const openButton = page.getByRole("button", { name: "Open photo viewer" });
  await openButton.click();

  const viewer = page.getByRole("dialog", { name: "Photo viewer" });
  await expect(viewer).toBeVisible();
  await expect(viewer.getByRole("link", { name: "Charles Babbage" })).toBeVisible();
  const closeButton = viewer.getByRole("button", { name: "Close photo viewer" });
  await expect(closeButton).toBeFocused();
  const closeBox = await closeButton.boundingBox();
  expect(closeBox?.y).toBeGreaterThanOrEqual(56);

  await page.keyboard.press("Escape");
  await expect(viewer).toBeHidden();
  await expect(openButton).toBeFocused();
});

test("family layout keeps spouses and sibling families together", () => {
  const node = (id: string, level: number) => ({ data: { id, level } });
  const edge = (
    id: string,
    source: string,
    target: string,
    type: "isSpouseOf" | "isChildOf",
  ) => ({ data: { id, source, target, type } });
  const elements = [
    node("parents-a-1", 0),
    node("parents-a-2", 0),
    node("parents-b-1", 0),
    node("parents-b-2", 0),
    node("sibling-a-1", 1),
    node("spouse-a", 1),
    node("sibling-b-1", 1),
    node("spouse-b", 1),
    node("sibling-a-2", 1),
    node("sibling-b-2", 1),
    edge("pa", "parents-a-1", "parents-a-2", "isSpouseOf"),
    edge("pb", "parents-b-1", "parents-b-2", "isSpouseOf"),
    edge("sa", "sibling-a-1", "spouse-a", "isSpouseOf"),
    edge("sb", "sibling-b-1", "spouse-b", "isSpouseOf"),
    edge("a11", "sibling-a-1", "parents-a-1", "isChildOf"),
    edge("a12", "sibling-a-1", "parents-a-2", "isChildOf"),
    edge("a21", "sibling-a-2", "parents-a-1", "isChildOf"),
    edge("a22", "sibling-a-2", "parents-a-2", "isChildOf"),
    edge("b11", "sibling-b-1", "parents-b-1", "isChildOf"),
    edge("b12", "sibling-b-1", "parents-b-2", "isChildOf"),
    edge("b21", "sibling-b-2", "parents-b-1", "isChildOf"),
    edge("b22", "sibling-b-2", "parents-b-2", "isChildOf"),
  ];
  const layout = buildHierarchicalLayout(elements, true) as unknown as {
    positions: (node: { id: () => string }) => { x: number; y: number };
  };
  const x = (id: string) => layout.positions({ id: () => id }).x;
  const childIds = elements
    .filter((element) => "level" in element.data && element.data.level === 1)
    .map((element) => element.data.id)
    .sort((a, b) => x(a) - x(b));
  const indexes = (ids: string[]) => ids
    .map((id) => childIds.indexOf(id))
    .sort((a, b) => a - b);
  const familyAIndexes = indexes(["sibling-a-1", "spouse-a", "sibling-a-2"]);
  const familyBIndexes = indexes(["sibling-b-1", "spouse-b", "sibling-b-2"]);

  expect(familyAIndexes[2] - familyAIndexes[0]).toBe(2);
  expect(familyBIndexes[2] - familyBIndexes[0]).toBe(2);
  expect(Math.abs(x("sibling-a-1") - x("spouse-a"))).toBe(85);
  expect(Math.abs(x("sibling-b-1") - x("spouse-b"))).toBe(85);

  const parentACenter = (x("parents-a-1") + x("parents-a-2")) / 2;
  const parentBCenter = (x("parents-b-1") + x("parents-b-2")) / 2;
  const childACenter = (x("sibling-a-1") + x("spouse-a") + x("sibling-a-2")) / 3;
  const childBCenter = (x("sibling-b-1") + x("spouse-b") + x("sibling-b-2")) / 3;
  expect(Math.sign(parentACenter - parentBCenter)).toBe(Math.sign(childACenter - childBCenter));
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
