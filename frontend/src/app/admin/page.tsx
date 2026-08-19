"use client";

import { useState, useEffect } from "react";
import { listUsers, addUser, updateUser, deleteUser } from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useToast } from "@/components/ToastProvider";

interface User {
  email: string;
  role: string;
}

export default function AdminPage() {
  const { t } = useI18n();
  const toast = useToast();
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Add form
  const [newEmail, setNewEmail] = useState("");
  const [newRole, setNewRole] = useState("user");
  const [adding, setAdding] = useState(false);

  // Edit state
  const [editingEmail, setEditingEmail] = useState<string | null>(null);
  const [editDraft, setEditDraft] = useState<User>({ email: "", role: "" });
  const [busyEmail, setBusyEmail] = useState<string | null>(null);

  const refresh = async () => {
    try {
      const data = await listUsers();
      setUsers(data);
      setError(null);
    } catch (err) {
      setError(String(err));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newEmail.trim()) return;
    setAdding(true);
    setError(null);
    try {
      await addUser(newEmail.trim(), newRole);
      setNewEmail("");
      setNewRole("user");
      await refresh();
      toast.success(t("toast.userAdded"));
    } catch (err) {
      setError(String(err));
      toast.error(t("toast.userAddFailed"));
    } finally {
      setAdding(false);
    }
  };

  const handleDelete = async (email: string) => {
    if (!confirm(`${t("admin.confirmRemove")} "${email}"?`)) return;
    if (busyEmail) return;
    setBusyEmail(email);
    setError(null);
    try {
      await deleteUser(email);
      await refresh();
      toast.success(t("toast.userDeleted"));
    } catch (err) {
      setError(String(err));
      toast.error(t("toast.userDeleteFailed"), {
        action: { label: t("toast.retry"), onClick: () => handleDelete(email) },
      });
    } finally {
      setBusyEmail(null);
    }
  };

  const startEdit = (user: User) => {
    setEditingEmail(user.email);
    setEditDraft({ ...user });
  };

  const cancelEdit = () => {
    setEditingEmail(null);
  };

  const handleSaveEdit = async () => {
    if (!editingEmail || busyEmail) return;
    setBusyEmail(editingEmail);
    setError(null);
    try {
      await updateUser(editingEmail, editDraft.email, editDraft.role);
      setEditingEmail(null);
      await refresh();
      toast.success(t("toast.userSaved"));
    } catch (err) {
      setError(String(err));
      toast.error(t("toast.userSaveFailed"), {
        action: { label: t("toast.retry"), onClick: handleSaveEdit },
      });
    } finally {
      setBusyEmail(null);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center text-gray-500">
        {t("admin.loading")}
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-3xl mx-auto p-3 sm:p-6 space-y-6">
        <h1 className="text-2xl font-bold text-gray-900">{t("admin.title")}</h1>

        {error && (
          <div className="bg-red-50 text-red-700 rounded-lg border border-red-200 p-3 text-sm">
            {error}
          </div>
        )}

        {/* Users table */}
        <div className="bg-white rounded-lg border shadow-sm overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b">
              <tr>
                <th className="text-left px-4 py-3 font-medium text-gray-600">
                  {t("admin.email")}
                </th>
                <th className="text-left px-4 py-3 font-medium text-gray-600 w-32">
                  {t("admin.role")}
                </th>
                <th className="text-right px-4 py-3 font-medium text-gray-600 w-40">
                  {t("admin.actions")}
                </th>
              </tr>
            </thead>
            <tbody className="divide-y">
              {users.map((user) => (
                <tr key={user.email} className="hover:bg-gray-50">
                  {editingEmail === user.email ? (
                    <>
                      <td className="px-4 py-2">
                        <input
                          type="email"
                          value={editDraft.email}
                          onChange={(e) => setEditDraft({ ...editDraft, email: e.target.value })}
                          className="w-full border rounded px-2 py-1 text-sm text-gray-900"
                        />
                      </td>
                      <td className="px-4 py-2">
                        <select
                          value={editDraft.role}
                          onChange={(e) => setEditDraft({ ...editDraft, role: e.target.value })}
                          className="border rounded px-2 py-1 text-sm text-gray-900"
                        >
                          <option value="user">{t("admin.roleUser")}</option>
                          <option value="admin">{t("admin.roleAdmin")}</option>
                        </select>
                      </td>
                      <td className="px-4 py-2 text-right">
                        <button
                          onClick={handleSaveEdit}
                          disabled={busyEmail === editingEmail}
                          className="text-xs px-2 py-1 bg-blue-600 text-white rounded hover:bg-blue-700 mr-1 disabled:cursor-not-allowed disabled:opacity-50"
                        >
                          {t("admin.save")}
                        </button>
                        <button
                          onClick={cancelEdit}
                          disabled={busyEmail === editingEmail}
                          className="text-xs px-2 py-1 border rounded hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
                        >
                          {t("admin.cancel")}
                        </button>
                      </td>
                    </>
                  ) : (
                    <>
                      <td className="px-4 py-3 text-gray-900">{user.email}</td>
                      <td className="px-4 py-3">
                        <span
                          className={`text-xs px-2 py-0.5 rounded-full ${
                            user.role === "admin"
                              ? "bg-purple-100 text-purple-700"
                              : "bg-gray-100 text-gray-700"
                          }`}
                        >
                          {user.role}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right">
                        <button
                          onClick={() => startEdit(user)}
                          className="text-xs px-2 py-1 border rounded text-blue-600 hover:bg-blue-50 mr-1"
                        >
                          {t("admin.edit")}
                        </button>
                        <button
                          onClick={() => handleDelete(user.email)}
                          disabled={busyEmail === user.email}
                          className="text-xs px-2 py-1 border rounded text-red-600 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
                        >
                          {t("admin.remove")}
                        </button>
                      </td>
                    </>
                  )}
                </tr>
              ))}
              {users.length === 0 && (
                <tr>
                  <td colSpan={3} className="px-4 py-6 text-center text-gray-400">
                    {t("admin.noUsers")}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {/* Add user form */}
        <div className="bg-white rounded-lg border shadow-sm p-4">
          <h2 className="font-semibold text-gray-900 mb-3">{t("admin.addUser")}</h2>
          <form onSubmit={handleAdd} className="flex items-end gap-3">
            <div className="flex-1">
              <label className="block text-xs font-medium text-gray-600 mb-1">{t("admin.email")}</label>
              <input
                type="email"
                value={newEmail}
                onChange={(e) => setNewEmail(e.target.value)}
                placeholder={t("admin.emailPlaceholder")}
                required
                className="w-full border rounded px-3 py-1.5 text-sm text-gray-900"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-600 mb-1">{t("admin.role")}</label>
              <select
                value={newRole}
                onChange={(e) => setNewRole(e.target.value)}
                className="border rounded px-3 py-1.5 text-sm text-gray-900"
              >
                <option value="user">{t("admin.roleUser")}</option>
                <option value="admin">{t("admin.roleAdmin")}</option>
              </select>
            </div>
            <button
              type="submit"
              disabled={adding || !newEmail.trim()}
              className="px-4 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700 disabled:opacity-50"
            >
              {adding ? t("admin.adding") : t("admin.add")}
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}
