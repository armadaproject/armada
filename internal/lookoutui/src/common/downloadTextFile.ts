export const downloadTextFile = (content: string, fileName: string, blobType: string) => {
  const element = document.createElement("a")
  const file = new Blob([content], {
    type: blobType,
  })
  element.href = URL.createObjectURL(file)
  element.download = fileName
  document.body.appendChild(element)
  element.click()
}
