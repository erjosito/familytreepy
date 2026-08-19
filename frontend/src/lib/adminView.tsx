"use client";

import { createContext, useContext, useState, type ReactNode } from "react";

interface AdminViewContextValue {
  /** True if the user is actually an admin */
  isAdmin: boolean;
  /** True if admin view is active (admin link, dev toggle visible) */
  adminView: boolean;
  /** Toggle between admin and user view */
  setAdminView: (v: boolean) => void;
  /** Current user's email */
  userEmail: string;
  /** Current user's display name */
  userName: string;
}

const AdminViewContext = createContext<AdminViewContextValue>({
  isAdmin: false,
  adminView: false,
  setAdminView: () => {},
  userEmail: "",
  userName: "",
});

export function AdminViewProvider({
  isAdmin,
  userEmail = "",
  userName = "",
  children,
}: {
  isAdmin: boolean;
  userEmail?: string;
  userName?: string;
  children: ReactNode;
}) {
  const [adminView, setAdminView] = useState(isAdmin);

  return (
    <AdminViewContext.Provider value={{ isAdmin, adminView, setAdminView, userEmail, userName }}>
      {children}
    </AdminViewContext.Provider>
  );
}

export function useAdminView() {
  return useContext(AdminViewContext);
}
