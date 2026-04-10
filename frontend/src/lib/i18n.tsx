"use client";

import { createContext, useContext, useState, useCallback, type ReactNode } from "react";

export type Locale = "en" | "es";

const translations = {
  en: {
    // Nav
    "nav.title": "🌳 Family Tree",
    "nav.explore": "Explore",
    "nav.image": "Image",
    "nav.devMode": "Dev Mode",
    "nav.signOut": "Sign out",

    // Auth
    "auth.loading": "Loading...",
    "auth.initializing": "Initializing...",
    "auth.title": "🌳 Family Tree",
    "auth.signInPrompt": "Sign in to continue",
    "auth.signIn": "Sign in with Microsoft",

    // Toolbar
    "toolbar.center": "Center:",
    "toolbar.all": "All",
    "toolbar.radius": "Radius:",
    "toolbar.layout": "Layout:",
    "toolbar.loading": "Loading...",

    // Layout modes
    "layout.hierarchical": "Hierarchical",
    "layout.radial": "Radial",
    "layout.forceDirected": "Force-directed",
    "layout.grid": "Grid",
    "layout.circle": "Circle",

    // Context menu
    "menu.addChild": "Add child",
    "menu.addSpouse": "Add spouse",
    "menu.addParent": "Add parent",
    "menu.editPerson": "Edit person",
    "menu.deletePerson": "Delete person",

    // Form titles
    "form.editPerson": "Edit Person",
    "form.addChild": "Add Child",
    "form.addSpouse": "Add Spouse",
    "form.addParent": "Add Parent",
    "form.create": "Create",
    "form.save": "Save",
    "form.cancel": "Cancel",

    // Confirm
    "confirm.deletePerson": "Delete this person?",

    // Detail panel
    "detail.selectPerson": "Select a person",
    "detail.selectHint": "Click on a node in the graph to see details",
    "detail.edit": "✏️ Edit",
    "detail.changePhoto": "📷 Change",
    "detail.saving": "Saving...",
    "detail.save": "💾 Save",
    "detail.cancel": "Cancel",

    // Fields
    "field.firstName": "First name",
    "field.lastName": "Last name",
    "field.birthdate": "Birthdate",
    "field.birthplace": "Birthplace",
    "field.alive": "Alive:",
    "field.deathDate": "Death date",
    "field.status": "Status:",
    "field.living": "Living",
    "field.deceased": "Deceased",
    "field.born": "Born:",
    "field.placeholderDate": "e.g. 1990-01-31",
    "field.placeholderDeathDate": "e.g. 2020-12-15",

    // Relationships
    "rel.title": "Relationships",
    "rel.parent": "Parent",
    "rel.child": "Child",
    "rel.spouse": "Spouse",
    "rel.inactive": "(inactive)",
    "rel.siblings": "Siblings",

    // Pictures
    "pic.title": "Pictures",
    "pic.addPhoto": "+ Add photo",
    "pic.tagPeople": "Tag people in this photo:",
    "pic.uploading": "Uploading...",
    "pic.upload": "⬆ Upload",
    "pic.cancel": "Cancel",
    "pic.remove": "Remove from this person",
    "pic.dragHint": "Drag to position · Scroll to zoom",
    "pic.uploadBtn": "✓ Upload",

    // Image page
    "image.title": "Generate Family Tree Image",
    "image.centerPerson": "Center Person",
    "image.selectPerson": "Select a person...",
    "image.degree": "Degree:",
    "image.layout": "Layout",
    "image.generating": "Generating...",
    "image.generate": "Generate Image",
    "image.preview": "Preview",
    "image.download": "⬇ Download PNG",

    // Nav (extra)
    "nav.admin": "Admin",
    "nav.switchToUser": "Switch to user view",
    "nav.switchToAdmin": "Switch to admin view",
    "nav.adminBadge": "👑 Admin",
    "nav.userBadge": "👤 User",

    // Detail panel (extra)
    "detail.viewProfile": "View full profile →",
    "detail.clear": "Clear",

    // Relationship toggle
    "rel.active": "Active",
    "rel.relInactive": "Inactive",
    "rel.deactivate": "Deactivate relationship",
    "rel.reactivate": "Reactivate relationship",

    // Notes
    "notes.title": "Notes",
    "notes.loading": "Loading...",
    "notes.placeholder": "Write a note...",
    "notes.adding": "Adding...",
    "notes.add": "Add note",
    "notes.delete": "Delete note",

    // Person tag search
    "tag.searchPlaceholder": "Type a name to search...",
    "tag.noMatches": "No matches",

    // Person page
    "person.loading": "Loading...",
    "person.notFound": "Person not found",
    "person.backToExplore": "← Explore",
    "person.born": "Born",
    "person.birthplace": "Birthplace",
    "person.status": "Status",
    "person.living": "Living",
    "person.deceased": "Deceased",
    "person.family": "Family",
    "person.parents": "Parents",
    "person.spouses": "Spouses",
    "person.children": "Children",
    "person.siblings": "Siblings",
    "person.pictures": "Pictures",
    "person.noPictures": "No pictures yet",

    // Admin page
    "admin.title": "User Management",
    "admin.loading": "Loading...",
    "admin.email": "Email",
    "admin.role": "Role",
    "admin.actions": "Actions",
    "admin.save": "Save",
    "admin.cancel": "Cancel",
    "admin.edit": "Edit",
    "admin.remove": "Remove",
    "admin.noUsers": "No users configured",
    "admin.addUser": "Add User",
    "admin.emailPlaceholder": "user@example.com",
    "admin.adding": "Adding...",
    "admin.add": "Add",
    "admin.confirmRemove": "Remove user",
    "admin.roleUser": "user",
    "admin.roleAdmin": "admin",

    // Dev
    "dev.nodeJson": "Node JSON",
    "dev.relJson": "Relationships JSON",
  },
  es: {
    // Nav
    "nav.title": "🌳 Árbol Familiar",
    "nav.explore": "Explorar",
    "nav.image": "Imagen",
    "nav.devMode": "Modo Dev",
    "nav.signOut": "Cerrar sesión",

    // Auth
    "auth.loading": "Cargando...",
    "auth.initializing": "Inicializando...",
    "auth.title": "🌳 Árbol Familiar",
    "auth.signInPrompt": "Inicia sesión para continuar",
    "auth.signIn": "Iniciar sesión con Microsoft",

    // Toolbar
    "toolbar.center": "Centro:",
    "toolbar.all": "Todos",
    "toolbar.radius": "Radio:",
    "toolbar.layout": "Disposición:",
    "toolbar.loading": "Cargando...",

    // Layout modes
    "layout.hierarchical": "Jerárquico",
    "layout.radial": "Radial",
    "layout.forceDirected": "Dirigido por fuerza",
    "layout.grid": "Cuadrícula",
    "layout.circle": "Círculo",

    // Context menu
    "menu.addChild": "Añadir hijo/a",
    "menu.addSpouse": "Añadir cónyuge",
    "menu.addParent": "Añadir progenitor/a",
    "menu.editPerson": "Editar persona",
    "menu.deletePerson": "Eliminar persona",

    // Form titles
    "form.editPerson": "Editar Persona",
    "form.addChild": "Añadir Hijo/a",
    "form.addSpouse": "Añadir Cónyuge",
    "form.addParent": "Añadir Progenitor/a",
    "form.create": "Crear",
    "form.save": "Guardar",
    "form.cancel": "Cancelar",

    // Confirm
    "confirm.deletePerson": "¿Eliminar esta persona?",

    // Detail panel
    "detail.selectPerson": "Selecciona una persona",
    "detail.selectHint": "Haz clic en un nodo del grafo para ver detalles",
    "detail.edit": "✏️ Editar",
    "detail.changePhoto": "📷 Cambiar",
    "detail.saving": "Guardando...",
    "detail.save": "💾 Guardar",
    "detail.cancel": "Cancelar",

    // Fields
    "field.firstName": "Nombre",
    "field.lastName": "Apellidos",
    "field.birthdate": "Fecha de nacimiento",
    "field.birthplace": "Lugar de nacimiento",
    "field.alive": "Vivo/a:",
    "field.deathDate": "Fecha de defunción",
    "field.status": "Estado:",
    "field.living": "Vivo/a",
    "field.deceased": "Fallecido/a",
    "field.born": "Nacimiento:",
    "field.placeholderDate": "ej. 1990-01-31",
    "field.placeholderDeathDate": "ej. 2020-12-15",

    // Relationships
    "rel.title": "Relaciones",
    "rel.parent": "Progenitor/a",
    "rel.child": "Hijo/a",
    "rel.spouse": "Cónyuge",
    "rel.inactive": "(inactiva)",
    "rel.siblings": "Hermanos/as",

    // Pictures
    "pic.title": "Fotos",
    "pic.addPhoto": "+ Añadir foto",
    "pic.tagPeople": "Etiquetar personas en esta foto:",
    "pic.uploading": "Subiendo...",
    "pic.upload": "⬆ Subir",
    "pic.cancel": "Cancelar",
    "pic.remove": "Eliminar de esta persona",
    "pic.dragHint": "Arrastra para posicionar · Desplaza para zoom",
    "pic.uploadBtn": "✓ Subir",

    // Image page
    "image.title": "Generar Imagen del Árbol Familiar",
    "image.centerPerson": "Persona Central",
    "image.selectPerson": "Selecciona una persona...",
    "image.degree": "Grado:",
    "image.layout": "Disposición",
    "image.generating": "Generando...",
    "image.generate": "Generar Imagen",
    "image.preview": "Vista previa",
    "image.download": "⬇ Descargar PNG",

    // Nav (extra)
    "nav.admin": "Admin",
    "nav.switchToUser": "Cambiar a vista de usuario",
    "nav.switchToAdmin": "Cambiar a vista de admin",
    "nav.adminBadge": "👑 Admin",
    "nav.userBadge": "👤 Usuario",

    // Detail panel (extra)
    "detail.viewProfile": "Ver perfil completo →",
    "detail.clear": "Limpiar",

    // Relationship toggle
    "rel.active": "Activa",
    "rel.relInactive": "Inactiva",
    "rel.deactivate": "Desactivar relación",
    "rel.reactivate": "Reactivar relación",

    // Notes
    "notes.title": "Notas",
    "notes.loading": "Cargando...",
    "notes.placeholder": "Escribe una nota...",
    "notes.adding": "Añadiendo...",
    "notes.add": "Añadir nota",
    "notes.delete": "Eliminar nota",

    // Person tag search
    "tag.searchPlaceholder": "Escribe un nombre para buscar...",
    "tag.noMatches": "Sin resultados",

    // Person page
    "person.loading": "Cargando...",
    "person.notFound": "Persona no encontrada",
    "person.backToExplore": "← Explorar",
    "person.born": "Nacimiento",
    "person.birthplace": "Lugar de nacimiento",
    "person.status": "Estado",
    "person.living": "Vivo/a",
    "person.deceased": "Fallecido/a",
    "person.family": "Familia",
    "person.parents": "Progenitores",
    "person.spouses": "Cónyuges",
    "person.children": "Hijos/as",
    "person.siblings": "Hermanos/as",
    "person.pictures": "Fotos",
    "person.noPictures": "Aún no hay fotos",

    // Admin page
    "admin.title": "Gestión de Usuarios",
    "admin.loading": "Cargando...",
    "admin.email": "Email",
    "admin.role": "Rol",
    "admin.actions": "Acciones",
    "admin.save": "Guardar",
    "admin.cancel": "Cancelar",
    "admin.edit": "Editar",
    "admin.remove": "Eliminar",
    "admin.noUsers": "No hay usuarios configurados",
    "admin.addUser": "Añadir Usuario",
    "admin.emailPlaceholder": "usuario@ejemplo.com",
    "admin.adding": "Añadiendo...",
    "admin.add": "Añadir",
    "admin.confirmRemove": "Eliminar usuario",
    "admin.roleAdmin": "admin",
    "admin.roleUser": "usuario",

    // Dev
    "dev.nodeJson": "JSON del nodo",
    "dev.relJson": "JSON de relaciones",
  },
} as const;

type TranslationKey = keyof typeof translations.en;

interface I18nContextValue {
  locale: Locale;
  setLocale: (locale: Locale) => void;
  t: (key: TranslationKey) => string;
}

const I18nContext = createContext<I18nContextValue | null>(null);

export function I18nProvider({ children }: { children: ReactNode }) {
  const [locale, setLocaleState] = useState<Locale>(() => {
    if (typeof window !== "undefined") {
      const saved = localStorage.getItem("locale") as Locale;
      if (saved === "en" || saved === "es") return saved;
    }
    return "en";
  });

  const setLocale = useCallback((l: Locale) => {
    setLocaleState(l);
    if (typeof window !== "undefined") localStorage.setItem("locale", l);
  }, []);

  const t = useCallback(
    (key: TranslationKey) => translations[locale][key] ?? key,
    [locale]
  );

  return (
    <I18nContext.Provider value={{ locale, setLocale, t }}>
      {children}
    </I18nContext.Provider>
  );
}

export function useI18n() {
  const ctx = useContext(I18nContext);
  if (!ctx) throw new Error("useI18n must be used within I18nProvider");
  return ctx;
}
