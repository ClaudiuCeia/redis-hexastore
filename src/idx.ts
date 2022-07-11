export const idx = <T>(value: T | undefined | null, reason?: string): T => {
  if (value === undefined || value === null) {
    throw new Error(`
      Expected value to be defined: ${reason}
    `);
  }

  return value;
};
