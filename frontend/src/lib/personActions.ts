import type { TranslationKey } from "@/lib/i18n";

export interface PersonActionDefinition {
  action: string;
  labelKey: TranslationKey;
  icon: string;
  adminOnly?: boolean;
  destructive?: boolean;
}

const PERSON_ACTIONS: PersonActionDefinition[] = [
  { action: "center", labelKey: "menu.centerOn", icon: "🎯" },
  { action: "story", labelKey: "story.viewStory", icon: "📖" },
  { action: "add_child", labelKey: "menu.addChild", icon: "➕" },
  { action: "add_spouse", labelKey: "menu.addSpouse", icon: "💑" },
  { action: "add_parent", labelKey: "menu.addParent", icon: "👆" },
  { action: "link_child", labelKey: "menu.linkChild", icon: "🔗" },
  { action: "link_spouse", labelKey: "menu.linkSpouse", icon: "🔗" },
  { action: "link_parent", labelKey: "menu.linkParent", icon: "🔗" },
  { action: "edit", labelKey: "menu.editPerson", icon: "✏️" },
  {
    action: "delete",
    labelKey: "menu.deletePerson",
    icon: "🗑",
    adminOnly: true,
    destructive: true,
  },
];

export function getPersonActions(adminView: boolean) {
  return PERSON_ACTIONS.filter((action) => !action.adminOnly || adminView);
}
