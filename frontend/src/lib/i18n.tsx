"use client";

import { createContext, useContext, useState, useCallback, useEffect, type ReactNode } from "react";

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
    "auth.signIn": "Sign in",
    "auth.signInMicrosoft": "Sign in with Microsoft",
    "auth.signInGoogle": "Sign in with Google",
    "auth.signInOther": "Sign in with Google or GitHub",
    "auth.notAuthorized": "Your account is not authorized to use this application.",
    "auth.contactAdmin": "Please contact the application administrator to request access.",
    "auth.tryDifferentAccount": "Try with a different account",
    "auth.signInError": "Sign-in error",
    "auth.tryAgain": "Try again",

    // Toolbar
    "toolbar.center": "Center:",
    "toolbar.all": "All",
    "toolbar.radius": "Radius:",
    "toolbar.layout": "Layout:",
    "toolbar.loading": "Loading...",
    "search.label": "Find a person",
    "search.placeholder": "Search name or alias...",
    "search.results": "Search results",
    "search.recent": "Recently viewed",
    "search.noResults": "No matching people",
    "search.noRecent": "Start typing to find someone",
    "search.unknown": "Unknown person",
    "actions.title": "Person actions",
    "actions.open": "More actions",
    "actions.close": "Close actions",

    // Layout modes
    "layout.family": "Family tree",
    "layout.legacyHierarchical": "Hierarchical (legacy)",
    "layout.radial": "Radial",
    "layout.forceDirected": "Force-directed",
    "layout.grid": "Grid",
    "layout.circle": "Circle",

    // Context menu
    "menu.centerOn": "Center on this person",
    "menu.addChild": "Add child",
    "menu.addSpouse": "Add spouse",
    "menu.addParent": "Add parent",
    "menu.editPerson": "Edit person",
    "menu.deletePerson": "Delete person",
    "menu.linkChild": "Link existing child",
    "menu.linkSpouse": "Link existing spouse",
    "menu.linkParent": "Link existing parent",
    "link.hint": "Search for a person to link as a relationship.",

    // Form titles
    "form.editPerson": "Edit Person",
    "form.addChild": "Add Child",
    "form.addSpouse": "Add Spouse",
    "form.addParent": "Add Parent",
    "form.create": "Create",
    "form.save": "Save",
    "form.saving": "Saving...",
    "form.cancel": "Cancel",
    "form.otherParent": "Other parent",
    "form.noOtherParent": "None (single parent)",
    "validation.errorTitle": "Please correct these issues",
    "validation.warningTitle": "Please review these warnings",
    "validation.saveAnyway": "Save anyway",
    "validation.confirmOverride": "Continue anyway?",

    // Confirm
    "confirm.deletePerson": "Delete this person?",

    // Detail panel
    "detail.selectPerson": "Select a person",
    "detail.selectHint": "Click on a node in the graph to see details",
    "detail.edit": "✏️ Edit",
    "detail.changePhoto": "📷 Change",
    "detail.removePhoto": "🗑 Remove",
    "detail.deletePhoto": "Remove the profile picture?",
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
    "field.gender": "Gender",
    "field.alias": "Alias",
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
    "rel.delete": "Delete relationship",
    "rel.confirmDelete": "Delete this relationship? This cannot be undone.",

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
    "image.colorScheme": "Color scheme",
    "image.advanced": "Advanced options",
    "image.canvasSize": "Canvas size",
    "image.fontScale": "Font scale",
    "image.lineWidth": "Line width",

    // Grid page
    "grid.title": "People",
    "grid.search": "Search by name, place, date...",

    // Nav (extra)
    "nav.admin": "Admin",
    "nav.grid": "Grid",
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

    // Notifications
    "toast.notifications": "Notifications",
    "toast.dismiss": "Dismiss notification",
    "toast.retry": "Retry",
    "toast.personCreated": "Person created",
    "toast.personSaved": "Person saved",
    "toast.personDeleted": "Person deleted",
    "toast.personCreateFailed": "Could not create the person",
    "toast.personSaveFailed": "Could not save the person",
    "toast.personDeleteFailed": "Could not delete the person",
    "toast.relationshipCreated": "Relationship created",
    "toast.relationshipUpdated": "Relationship updated",
    "toast.relationshipDeleted": "Relationship deleted",
    "toast.relationshipCreateFailed": "Could not create the relationship",
    "toast.relationshipUpdateFailed": "Could not update the relationship",
    "toast.relationshipDeleteFailed": "Could not delete the relationship",
    "toast.noteAdded": "Note added",
    "toast.noteDeleted": "Note deleted",
    "toast.noteAddFailed": "Could not add the note",
    "toast.noteDeleteFailed": "Could not delete the note",
    "toast.photoUploaded": "Photo uploaded",
    "toast.photoRemoved": "Photo removed",
    "toast.photoUpdated": "Profile photo updated",
    "toast.photoUploadFailed": "Could not upload the photo",
    "toast.photoRemoveFailed": "Could not remove the photo",
    "toast.photoTagUpdated": "Photo tags updated",
    "toast.photoTagFailed": "Could not update photo tags",
    "toast.userAdded": "User added",
    "toast.userSaved": "User updated",
    "toast.userDeleted": "User removed",
    "toast.userAddFailed": "Could not add the user",
    "toast.userSaveFailed": "Could not update the user",
    "toast.userDeleteFailed": "Could not remove the user",
    "toast.imageGenerated": "Family tree image generated",
    "toast.imageGenerateFailed": "Could not generate the image",
    "toast.loadFailed": "Could not load the requested data",

    // Geni
    "nav.geni": "Geni",
    "geni.title": "Geni.com Search",
    "geni.connect": "Connect to Geni",
    "geni.connected": "Connected to Geni",
    "geni.search": "Search by name...",
    "geni.searching": "Searching...",
    "geni.noResults": "No profiles found",
    "geni.import": "Import",
    "geni.importFamily": "Import with family",
    "geni.importing": "Importing...",
    "geni.imported": "Imported!",
    "geni.viewProfile": "View profile",
    "geni.family": "Family",
    "geni.parents": "Parents",
    "geni.spouses": "Spouses",
    "geni.children": "Children",

    // Story
    "story.loading": "Preparing your family story...",
    "story.title": "Family Story",
    "story.play": "Play",
    "story.pause": "Pause",
    "story.exit": "Exit",
    "story.speed": "Speed",
    "story.generation": "Generation",
    "story.theirDaughter": "Their daughter...",
    "story.theirSon": "Their son...",
    "story.theirChild": "Their child...",
    "story.marriedTo": "married",
    "story.herParent": "Her parent...",
    "story.hisParent": "His parent...",
    "story.theirSister": "Their sister...",
    "story.theirBrother": "Their brother...",
    "story.nextInFamily": "Also in the family...",
    "story.daughter": "daughter of",
    "story.son": "son of",
    "story.motherOf": "mother of",
    "story.fatherOf": "father of",
    "story.sisterOf": "sister of",
    "story.brotherOf": "brother of",
    "story.grandmotherOf": "grandmother of",
    "story.grandfatherOf": "grandfather of",
    "story.ancestorOf": "ancestor of",
    "story.spouseOf": "spouse of",
    "story.granddaughterOf": "granddaughter of",
    "story.grandsonOf": "grandson of",
    "story.descendantOf": "descendant of",
    "story.viewStory": "View story",
    "story.protagonist": "our protagonist",
    "story.protagonistM": "our protagonist",
    "story.protagonistF": "our protagonist",
    "story.and": "and",
    "story.familyOverview": "Family Overview",
    "story.child": "child",
    "story.children": "children",
    "story.noSlides": "No slides to show",
    "story.familyDefault": "Family",

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
    "auth.signIn": "Iniciar sesión",
    "auth.signInMicrosoft": "Iniciar sesión con Microsoft",
    "auth.signInGoogle": "Iniciar sesión con Google",
    "auth.signInOther": "Iniciar sesión con Google o GitHub",
    "auth.notAuthorized": "Tu cuenta no está autorizada para usar esta aplicación.",
    "auth.contactAdmin": "Contacta al administrador de la aplicación para solicitar acceso.",
    "auth.tryDifferentAccount": "Intentar con otra cuenta",
    "auth.signInError": "Error de inicio de sesión",
    "auth.tryAgain": "Intentar de nuevo",

    // Toolbar
    "toolbar.center": "Centro:",
    "toolbar.all": "Todos",
    "toolbar.radius": "Radio:",
    "toolbar.layout": "Disposición:",
    "toolbar.loading": "Cargando...",
    "search.label": "Buscar una persona",
    "search.placeholder": "Buscar nombre o alias...",
    "search.results": "Resultados de búsqueda",
    "search.recent": "Vistos recientemente",
    "search.noResults": "No hay personas que coincidan",
    "search.noRecent": "Escribe para buscar a alguien",
    "search.unknown": "Persona desconocida",
    "actions.title": "Acciones de la persona",
    "actions.open": "Más acciones",
    "actions.close": "Cerrar acciones",

    // Layout modes
    "layout.family": "Árbol familiar",
    "layout.legacyHierarchical": "Jerárquico (anterior)",
    "layout.radial": "Radial",
    "layout.forceDirected": "Dirigido por fuerza",
    "layout.grid": "Cuadrícula",
    "layout.circle": "Círculo",

    // Context menu
    "menu.centerOn": "Centrar en esta persona",
    "menu.addChild": "Añadir hijo/a",
    "menu.addSpouse": "Añadir cónyuge",
    "menu.addParent": "Añadir progenitor/a",
    "menu.editPerson": "Editar persona",
    "menu.deletePerson": "Eliminar persona",
    "menu.linkChild": "Vincular hijo/a existente",
    "menu.linkSpouse": "Vincular cónyuge existente",
    "menu.linkParent": "Vincular progenitor/a existente",
    "link.hint": "Busca una persona para vincular como relación.",

    // Form titles
    "form.editPerson": "Editar Persona",
    "form.addChild": "Añadir Hijo/a",
    "form.addSpouse": "Añadir Cónyuge",
    "form.addParent": "Añadir Progenitor/a",
    "form.create": "Crear",
    "form.save": "Guardar",
    "form.saving": "Guardando...",
    "form.cancel": "Cancelar",
    "form.otherParent": "Otro progenitor",
    "form.noOtherParent": "Ninguno (monoparental)",
    "validation.errorTitle": "Corrige estos problemas",
    "validation.warningTitle": "Revisa estas advertencias",
    "validation.saveAnyway": "Guardar de todos modos",
    "validation.confirmOverride": "¿Continuar de todos modos?",

    // Confirm
    "confirm.deletePerson": "¿Eliminar esta persona?",

    // Detail panel
    "detail.selectPerson": "Selecciona una persona",
    "detail.selectHint": "Haz clic en un nodo del grafo para ver detalles",
    "detail.edit": "✏️ Editar",
    "detail.changePhoto": "📷 Cambiar",
    "detail.removePhoto": "🗑 Eliminar",
    "detail.deletePhoto": "¿Eliminar la foto de perfil?",
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
    "field.gender": "Género",
    "field.alias": "Alias",
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
    "rel.delete": "Eliminar relación",
    "rel.confirmDelete": "¿Eliminar esta relación? Esta acción no se puede deshacer.",

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
    "image.colorScheme": "Esquema de color",
    "image.advanced": "Opciones avanzadas",
    "image.canvasSize": "Tamaño del lienzo",
    "image.fontScale": "Escala de fuente",
    "image.lineWidth": "Grosor de línea",

    // Grid page
    "grid.title": "Personas",
    "grid.search": "Buscar por nombre, lugar, fecha...",

    // Nav (extra)
    "nav.admin": "Admin",
    "nav.grid": "Tabla",
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

    // Notifications
    "toast.notifications": "Notificaciones",
    "toast.dismiss": "Cerrar notificación",
    "toast.retry": "Reintentar",
    "toast.personCreated": "Persona creada",
    "toast.personSaved": "Persona guardada",
    "toast.personDeleted": "Persona eliminada",
    "toast.personCreateFailed": "No se pudo crear la persona",
    "toast.personSaveFailed": "No se pudo guardar la persona",
    "toast.personDeleteFailed": "No se pudo eliminar la persona",
    "toast.relationshipCreated": "Relación creada",
    "toast.relationshipUpdated": "Relación actualizada",
    "toast.relationshipDeleted": "Relación eliminada",
    "toast.relationshipCreateFailed": "No se pudo crear la relación",
    "toast.relationshipUpdateFailed": "No se pudo actualizar la relación",
    "toast.relationshipDeleteFailed": "No se pudo eliminar la relación",
    "toast.noteAdded": "Nota añadida",
    "toast.noteDeleted": "Nota eliminada",
    "toast.noteAddFailed": "No se pudo añadir la nota",
    "toast.noteDeleteFailed": "No se pudo eliminar la nota",
    "toast.photoUploaded": "Foto subida",
    "toast.photoRemoved": "Foto eliminada",
    "toast.photoUpdated": "Foto de perfil actualizada",
    "toast.photoUploadFailed": "No se pudo subir la foto",
    "toast.photoRemoveFailed": "No se pudo eliminar la foto",
    "toast.photoTagUpdated": "Etiquetas de foto actualizadas",
    "toast.photoTagFailed": "No se pudieron actualizar las etiquetas",
    "toast.userAdded": "Usuario añadido",
    "toast.userSaved": "Usuario actualizado",
    "toast.userDeleted": "Usuario eliminado",
    "toast.userAddFailed": "No se pudo añadir el usuario",
    "toast.userSaveFailed": "No se pudo actualizar el usuario",
    "toast.userDeleteFailed": "No se pudo eliminar el usuario",
    "toast.imageGenerated": "Imagen del árbol familiar generada",
    "toast.imageGenerateFailed": "No se pudo generar la imagen",
    "toast.loadFailed": "No se pudieron cargar los datos solicitados",

    // Geni
    "nav.geni": "Geni",
    "geni.title": "Búsqueda en Geni.com",
    "geni.connect": "Conectar con Geni",
    "geni.connected": "Conectado a Geni",
    "geni.search": "Buscar por nombre...",
    "geni.searching": "Buscando...",
    "geni.noResults": "No se encontraron perfiles",
    "geni.import": "Importar",
    "geni.importFamily": "Importar con familia",
    "geni.importing": "Importando...",
    "geni.imported": "¡Importado!",
    "geni.viewProfile": "Ver perfil",
    "geni.family": "Familia",
    "geni.parents": "Progenitores",
    "geni.spouses": "Cónyuges",
    "geni.children": "Hijos/as",

    // Story
    "story.loading": "Preparando la historia familiar...",
    "story.title": "Historia Familiar",
    "story.play": "Reproducir",
    "story.pause": "Pausar",
    "story.exit": "Salir",
    "story.speed": "Velocidad",
    "story.generation": "Generación",
    "story.theirDaughter": "Su hija...",
    "story.theirSon": "Su hijo...",
    "story.theirChild": "Su hijo/a...",
    "story.marriedTo": "se casó con",
    "story.herParent": "Su madre/padre...",
    "story.hisParent": "Su madre/padre...",
    "story.theirSister": "Su hermana...",
    "story.theirBrother": "Su hermano...",
    "story.nextInFamily": "También en la familia...",
    "story.daughter": "hija de",
    "story.son": "hijo de",
    "story.motherOf": "madre de",
    "story.fatherOf": "padre de",
    "story.sisterOf": "hermana de",
    "story.brotherOf": "hermano de",
    "story.grandmotherOf": "abuela de",
    "story.grandfatherOf": "abuelo de",
    "story.ancestorOf": "antepasado de",
    "story.spouseOf": "cónyuge de",
    "story.granddaughterOf": "nieta de",
    "story.grandsonOf": "nieto de",
    "story.descendantOf": "descendiente de",
    "story.viewStory": "Ver historia",
    "story.protagonist": "nuestro/a protagonista",
    "story.protagonistM": "nuestro protagonista",
    "story.protagonistF": "nuestra protagonista",
    "story.and": "y",
    "story.familyOverview": "Vista General de la Familia",
    "story.child": "hijo/a",
    "story.children": "hijos/as",
    "story.noSlides": "No hay diapositivas",
    "story.familyDefault": "Familia",

    // Dev
    "dev.nodeJson": "JSON del nodo",
    "dev.relJson": "JSON de relaciones",
  },
} as const;

export type TranslationKey = keyof typeof translations.en;

interface I18nContextValue {
  locale: Locale;
  setLocale: (locale: Locale) => void;
  t: (key: TranslationKey) => string;
}

const I18nContext = createContext<I18nContextValue | null>(null);

export function I18nProvider({ children }: { children: ReactNode }) {
  const [locale, setLocaleState] = useState<Locale>("en");

  // Sync from localStorage after mount to avoid SSR hydration mismatch
  useEffect(() => {
    const saved = localStorage.getItem("locale") as Locale;
    if (saved === "en" || saved === "es") setLocaleState(saved);
  }, []);

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
