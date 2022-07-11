export type RequireExactlyOne<
  ObjectType,
  KeysType extends keyof ObjectType = keyof ObjectType
> = {
  [Key in KeysType]: Required<Pick<ObjectType, Key>> &
    Partial<Record<Exclude<KeysType, Key>, never>>;
}[KeysType] &
  Omit<ObjectType, KeysType>;
