import { useState } from "react";

export default function Sidebar({
  open,
  onClose,
  filters,
  selectedLocations,
  onLocationsChange,
  dateRange,
  onDateRangeChange,
}) {
  const [search, setSearch] = useState("");

  const allLocations = filters?.locations ?? [];
  const filtered = allLocations.filter((l) =>
    l.toLowerCase().includes(search.toLowerCase()),
  );

  const toggle = (loc) => {
    onLocationsChange(
      selectedLocations.includes(loc)
        ? selectedLocations.filter((l) => l !== loc)
        : [...selectedLocations, loc],
    );
  };

  return (
    <>
      {open && (
        <div
          className="fixed inset-0 bg-black/50 z-40 backdrop-blur-sm"
          onClick={onClose}
        />
      )}

      <div
        className={`fixed top-0 left-0 h-full w-80 bg-slate-900 border-r border-slate-700/80 z-50
                     transform transition-transform duration-300 ease-in-out overflow-y-auto
                     ${open ? "translate-x-0" : "-translate-x-full"}`}
      >
        <div className="p-6">
          <div className="flex items-center justify-between mb-8">
            <h2 className="text-lg font-bold text-white">Filters</h2>
            <button
              onClick={onClose}
              className="text-slate-400 hover:text-white transition text-xl leading-none"
            >
              &times;
            </button>
          </div>

          <div className="mb-8">
            <label className="text-sm font-medium text-slate-300 mb-2 block">
              Location
            </label>
            <input
              type="text"
              placeholder="Search locations\u2026"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full bg-slate-800 border border-slate-600 rounded-lg px-3 py-2 text-sm
                         text-slate-200 placeholder-slate-500 focus:outline-none focus:border-blue-500 mb-3"
            />
            <div className="max-h-52 overflow-y-auto space-y-0.5 pr-1">
              {filtered.map((loc) => (
                <label
                  key={loc}
                  className="flex items-center gap-2.5 text-sm text-slate-300 hover:text-white
                             cursor-pointer py-1.5 px-1 rounded hover:bg-slate-800/60 transition"
                >
                  <input
                    type="checkbox"
                    checked={selectedLocations.includes(loc)}
                    onChange={() => toggle(loc)}
                    className="rounded border-slate-500 text-blue-500 focus:ring-blue-500 focus:ring-offset-0
                               bg-slate-700"
                  />
                  <span className="truncate">{loc}</span>
                </label>
              ))}
            </div>
            {selectedLocations.length > 0 && (
              <button
                onClick={() => onLocationsChange([])}
                className="text-xs text-blue-400 mt-2 hover:underline"
              >
                Clear all ({selectedLocations.length})
              </button>
            )}
          </div>

          <div className="mb-8">
            <label className="text-sm font-medium text-slate-300 mb-2 block">
              Posted Between
            </label>
            <div className="space-y-2">
              <input
                type="date"
                value={dateRange.from}
                onChange={(e) =>
                  onDateRangeChange({ ...dateRange, from: e.target.value })
                }
                min={filters?.date_range?.min}
                max={filters?.date_range?.max}
                className="w-full bg-slate-800 border border-slate-600 rounded-lg px-3 py-2 text-sm
                           text-slate-200 focus:outline-none focus:border-blue-500"
              />
              <input
                type="date"
                value={dateRange.to}
                onChange={(e) =>
                  onDateRangeChange({ ...dateRange, to: e.target.value })
                }
                min={filters?.date_range?.min}
                max={filters?.date_range?.max}
                className="w-full bg-slate-800 border border-slate-600 rounded-lg px-3 py-2 text-sm
                           text-slate-200 focus:outline-none focus:border-blue-500"
              />
            </div>
            {(dateRange.from || dateRange.to) && (
              <button
                onClick={() => onDateRangeChange({ from: "", to: "" })}
                className="text-xs text-blue-400 mt-2 hover:underline"
              >
                Clear dates
              </button>
            )}
          </div>

          <div className="border-t border-slate-700/60 pt-4">
            <p className="text-xs text-slate-500">
              Tech Job Market Analytics v2.0
            </p>
          </div>
        </div>
      </div>
    </>
  );
}
