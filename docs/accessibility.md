# Accessibility

Family Tree targets WCAG 2.2 Level AA. Automated checks run in the deployment
workflow with Playwright and axe-core on the Explore, Person, Image, Grid, and
Admin routes.

## Keyboard controls

- Use the skip link to move directly to the page content.
- Open the mobile navigation menu with Enter or Space and close it with Escape.
- In person search, use Up/Down to choose a result, Enter to select it, and
  Escape to close the results.
- Focus the family graph with Tab. Use the arrow keys to move between people,
  Enter or Space to open details, `C` to center the selected person, and
  Shift+F10 to open person actions.
- In a person action menu, use Up/Down, Home, End, Enter, and Escape.
- Open a gallery photo with Enter or Space. In the photo viewer, use the
  tagged-person links normally and press Escape or the close button to return
  focus to the gallery thumbnail.

## Manual release checks

These checks complement automation and should be repeated when navigation,
forms, dialogs, graph interaction, or responsive layout changes.

1. At 320 CSS pixels and at 200% browser zoom, verify that every control remains
   reachable and that horizontal scrolling is confined to data tables.
2. Navigate each primary route without a pointer. Verify logical tab order,
   visible focus, Escape behavior, and focus return after dialogs and menus.
3. With NVDA/Firefox or VoiceOver/Safari, verify landmarks, headings, form
   labels, validation errors, graph instructions, selected-person
   announcements, and toast messages.
4. Enable reduced motion and confirm that graph centering and UI transitions
   do not animate.
5. Enable a forced-colors/high-contrast theme and confirm that controls,
   selection, status text, and relationship labels remain distinguishable.
6. Select a graph person and confirm that direct relatives and connecting
   relationships remain prominent while unrelated people are visibly muted.

Run the automated checks locally after creating a production build:

```powershell
Set-Location frontend
npm run build
npm run test:a11y
```
