"use client";

interface MenuItem {
  label: string;
  action: string;
}

interface Props {
  x: number;
  y: number;
  items: MenuItem[];
  onSelect: (action: string) => void;
  onClose: () => void;
}

export default function ContextMenu({ x, y, items, onSelect, onClose }: Props) {
  return (
    <>
      {/* Backdrop to close menu */}
      <div className="fixed inset-0 z-40" onClick={onClose} onContextMenu={(e) => { e.preventDefault(); onClose(); }} />
      <div
        className="fixed z-50 bg-white rounded-lg shadow-lg border py-1 min-w-[160px]"
        style={{ left: x, top: y }}
      >
        {items.map((item) => (
          <button
            key={item.action}
            className="w-full text-left px-4 py-2 text-sm text-gray-900 hover:bg-blue-50 transition-colors"
            onClick={() => {
              onSelect(item.action);
              onClose();
            }}
          >
            {item.label}
          </button>
        ))}
      </div>
    </>
  );
}
