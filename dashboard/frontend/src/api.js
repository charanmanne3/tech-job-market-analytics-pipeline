const BASE = "/api";

export async function fetchDashboard({ locations = [], dateFrom, dateTo } = {}) {
  const url = new URL(`${BASE}/dashboard`, window.location.origin);
  locations.forEach((l) => url.searchParams.append("locations", l));
  if (dateFrom) url.searchParams.set("date_from", dateFrom);
  if (dateTo) url.searchParams.set("date_to", dateTo);

  const res = await fetch(url);
  if (!res.ok) throw new Error("Failed to fetch dashboard data");
  return res.json();
}

export async function fetchFilters() {
  const res = await fetch(`${BASE}/filters`);
  if (!res.ok) throw new Error("Failed to fetch filters");
  return res.json();
}
