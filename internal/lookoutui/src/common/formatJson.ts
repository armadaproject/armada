// Re-indents JSON text so that it can be read, or returns undefined if the text is not JSON. It only changes
// whitespace and does not depend on the shape of the JSON, so every field is kept, including ones that nothing in
// the UI knows about.
export const tryFormatJson = (text: string): string | undefined => {
  try {
    return JSON.stringify(JSON.parse(text), undefined, 2)
  } catch {
    return undefined
  }
}
