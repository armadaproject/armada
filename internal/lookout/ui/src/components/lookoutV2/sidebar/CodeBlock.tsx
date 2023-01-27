import styles from "./CodeBlock.module.css"

export type CodeBlockProps = {
  text: string
}

export const CodeBlock = ({ text }: CodeBlockProps) => {
  return <pre className={styles.codeBlock}>{text}</pre>
}
