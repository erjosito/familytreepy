"use client";

import Link from "next/link";
import { isAuthEnabled } from "@/lib/auth";

function AuthenticatedNav() {
  // Dynamically import MSAL hooks only when auth is enabled
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { useIsAuthenticated, useMsal } = require("@azure/msal-react");
  const { instance, accounts } = useMsal();
  const isAuthenticated = useIsAuthenticated();

  if (!isAuthenticated) return null;

  const account = accounts[0];
  const name = account?.name || account?.username || "User";

  return (
    <div className="flex items-center gap-3">
      <span className="text-sm text-gray-700">{name}</span>
      <button
        className="text-sm text-red-500 hover:underline"
        onClick={() => instance.logoutRedirect()}
      >
        Sign out
      </button>
    </div>
  );
}

export default function NavBar() {
  return (
    <nav className="flex items-center gap-4 px-4 py-2 bg-white border-b shadow-sm">
      <Link href="/" className="text-lg font-bold text-blue-600">
        🌳 Family Tree
      </Link>

      <div className="flex items-center gap-3 text-sm">
        <Link href="/" className="text-gray-700 hover:text-blue-600">
          Explore
        </Link>
        <Link href="/image/" className="text-gray-700 hover:text-blue-600">
          Image
        </Link>
      </div>

      <div className="ml-auto">
        {isAuthEnabled() ? (
          <AuthenticatedNav />
        ) : (
          <span className="text-xs text-gray-400 bg-gray-100 px-2 py-1 rounded">
            Dev Mode
          </span>
        )}
      </div>
    </nav>
  );
}
