import { useCallback } from "react"

import { Download } from "@mui/icons-material"
import { IconButton, styled, useColorScheme } from "@mui/material"
import { Highlight, themes } from "prism-react-renderer"
import Prism from "prismjs"
import "prismjs/components/prism-bash"
import "prismjs/components/prism-yaml"

import { CopyIconButton } from "./CopyIconButton"

// All langauges in this set must be imported from Prism in the form:
// import "prismjs/components/prism-{language}"
type SupportedLanguage = "bash" | "yaml"

const DARK_PRISM_THEME = themes.oneDark
const LIGHT_PRISM_THEME = themes.oneLight

const CodeActionsContainer = styled("div")({
  display: "flex",
  position: "absolute",
  top: 10,
  right: 10,
  opacity: 0,
  transition: "opacity 300ms ease-in-out",
})

const CodeBlockContainer = styled("div")({
  position: "relative",

  "&:hover > .codeActionsContainer": {
    opacity: 1,
  },
})

const StyledPre = styled("pre")(({ theme }) => ({
  lineHeight: 1.2,
  fontSize: theme.typography.body2.fontSize,
  overflow: "auto",
  padding: 5,
  borderRadius: 5,
  minHeight: 50,
  display: "flex",
  alignItems: "center",
}))

const Code = styled("code")({
  display: "table",
  wordWrap: "break-word",
})

const CodeLine = styled("div")({
  display: "table-row",
  counterIncrement: "line-count",
})

const CodeLineNumber = styled("span")({
  display: "table-cell",
  textAlign: "right",
  width: "1%",
  position: "sticky",
  left: 0,
  padding: "0 12px",
  overflowWrap: "normal",

  "&::before": {
    content: "counter(line-count)",
    opacity: 0.4,
  },
})

export type CodeBlockProps = {
  language: SupportedLanguage | "text"
  code: string
  showLineNumbers: boolean
} & (
  | {
      downloadable: true
      downloadBlobType: string
      downloadFileName: string
    }
  | { downloadable: false; downloadBlobType?: undefined | string; downloadFileName?: undefined | string }
)

export const CodeBlock = ({
  language,
  code,
  showLineNumbers,
  downloadable,
  downloadBlobType,
  downloadFileName,
}: CodeBlockProps) => {
  const { colorScheme } = useColorScheme()

  const downloadFile = useCallback(() => {
    if (!downloadable) {
      return
    }

    const element = document.createElement("a")
    const file = new Blob([code], {
      type: downloadBlobType,
    })
    element.href = URL.createObjectURL(file)
    element.download = downloadFileName
    document.body.appendChild(element)
    element.click()
  }, [code, downloadable, downloadBlobType, downloadFileName])

  return (
    <CodeBlockContainer>
      <CodeActionsContainer className="codeActionsContainer">
        <CopyIconButton size="small" content={code} />
        {downloadable && (
          <IconButton size="small" title={`Download as ${language} file`} onClick={downloadFile}>
            <Download />
          </IconButton>
        )}
      </CodeActionsContainer>
      <Highlight
        prism={Prism}
        theme={colorScheme === "dark" ? DARK_PRISM_THEME : LIGHT_PRISM_THEME}
        language={language}
        code={code}
      >
        {({ style, tokens, getLineProps, getTokenProps }) => (
          <StyledPre style={style}>
            <Code>
              {tokens.map((line, i) => {
                const lineTokens = line.map((token, key) => <span key={key} {...getTokenProps({ token })} />)
                return showLineNumbers ? (
                  <CodeLine key={i} {...getLineProps({ line })}>
                    <CodeLineNumber />
                    <span>{lineTokens}</span>
                  </CodeLine>
                ) : (
                  <div key={i} {...getLineProps({ line })}>
                    {lineTokens}
                  </div>
                )
              })}
            </Code>
          </StyledPre>
        )}
      </Highlight>
    </CodeBlockContainer>
  )
}
