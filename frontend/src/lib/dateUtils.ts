/**
 * Format a date string for display as dd/mm/yyyy.
 * Accepts YYYY-MM-DD, DD/MM/YYYY, or other parseable formats.
 * Returns the original string if parsing fails.
 */
export function formatDate(dateStr: string | undefined | null): string {
  if (!dateStr) return "";
  // If already dd/mm/yyyy, return as-is
  if (/^\d{2}\/\d{2}\/\d{4}$/.test(dateStr)) return dateStr;
  // Try parsing YYYY-MM-DD
  const isoMatch = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) return `${isoMatch[3]}/${isoMatch[2]}/${isoMatch[1]}`;
  // Try parsing as Date object
  const d = new Date(dateStr);
  if (!isNaN(d.getTime())) {
    const day = String(d.getDate()).padStart(2, "0");
    const month = String(d.getMonth() + 1).padStart(2, "0");
    const year = d.getFullYear();
    return `${day}/${month}/${year}`;
  }
  return dateStr;
}

/**
 * Format a timestamp for display (for notes, etc.)
 */
export function formatTimestamp(ts: string): string {
  try {
    return new Date(ts).toLocaleDateString("es-ES", {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return ts;
  }
}
