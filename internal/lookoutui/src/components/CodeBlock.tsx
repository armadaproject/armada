import { ReactNode, useCallback } from "react"

import { Download } from "@mui/icons-material"
import { IconButton, Skeleton, styled, useColorScheme } from "@mui/material"
import { Highlight, themes } from "prism-react-renderer"
import Prism from "prismjs"
import "prismjs/components/prism-bash"
import "prismjs/components/prism-yaml"

import { CopyIconButton } from "./CopyIconButton"
import { useCodeSnippetsWrapLines } from "../userSettings"

// All langauges in this set must be imported from Prism in the form:
// import "prismjs/components/prism-{language}"
type SupportedLanguage = "bash" | "yaml"

const DEFAULT_LOADING_LINES = 20
const DEFAULT_LOADING_LINE_LENGTH = 80

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

const StyledPre = styled("pre")<{ wrap: boolean }>(({ theme, wrap }) => ({
  lineHeight: 1.2,
  fontSize: theme.typography.body2.fontSize,
  overflow: "auto",
  padding: 5,
  borderRadius: 5,
  minHeight: 50,
  display: "flex",
  alignItems: "center",
  margin: 0,
  textWrap: wrap ? "wrap" : undefined,
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

interface CodeBlockActionProps {
  title: string
  onClick: () => void
  icon: ReactNode
}

interface CodeBlockLoadingProps {
  loading: true
  code?: undefined | string
  language?: undefined | SupportedLanguage | "text"
}

interface CodeBlockLoadedProps {
  loading: false
  code: string
  language: SupportedLanguage | "text"
}

interface CodeBlockDownloadbaleProps {
  downloadable: true
  downloadBlobType: string
  downloadFileName: string
}

interface CodeBlockNonDownloadbaleProps {
  downloadable: false
  downloadBlobType?: undefined | string
  downloadFileName?: undefined | string
}

interface CodeBlockBaseProps {
  showLineNumbers: boolean
  loadingLines?: number
  loadingLineLength?: number
  additionalActions?: CodeBlockActionProps[]
}

export type CodeBlockProps = CodeBlockBaseProps &
  (CodeBlockLoadedProps | CodeBlockLoadingProps) &
  (CodeBlockDownloadbaleProps | CodeBlockNonDownloadbaleProps)

export const CodeBlock = ({
  language,
  code,
  loading,
  downloadable,
  downloadBlobType,
  downloadFileName,
  showLineNumbers,
  loadingLines = DEFAULT_LOADING_LINES,
  loadingLineLength = DEFAULT_LOADING_LINE_LENGTH,
  additionalActions = [],
}: CodeBlockProps) => {
  const { colorScheme } = useColorScheme()
  const [wrapLines] = useCodeSnippetsWrapLines()

  const downloadFile = useCallback(() => {
    if (!downloadable || loading) {
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

  if (loading) {
    return (
      <CodeBlockContainer>
        <Highlight
          prism={Prism}
          theme={colorScheme === "dark" ? DARK_PRISM_THEME : LIGHT_PRISM_THEME}
          language="text"
          code={Array(loadingLines).fill("").join("\n")}
        >
          {({ style, tokens, getLineProps }) => (
            <StyledPre style={style} wrap={wrapLines}>
              <Code>
                {tokens.map((line, i) =>
                  showLineNumbers ? (
                    <CodeLine key={i} {...getLineProps({ line })}>
                      <CodeLineNumber />
                      <span>
                        <Skeleton>{Array(loadingLineLength).fill(" ").join("")}</Skeleton>
                      </span>
                    </CodeLine>
                  ) : (
                    <div key={i} {...getLineProps({ line })}>
                      <Skeleton>{Array(loadingLineLength).fill(" ").join("")}</Skeleton>
                    </div>
                  ),
                )}
              </Code>
            </StyledPre>
          )}
        </Highlight>
      </CodeBlockContainer>
    )
  }

  return (
    <CodeBlockContainer>
      <CodeActionsContainer className="codeActionsContainer">
        <CopyIconButton size="small" content={code} />
        {downloadable && (
          <IconButton size="small" title={`Download as ${language} file`} onClick={downloadFile}>
            <Download />
          </IconButton>
        )}
        {additionalActions.map(({ title, onClick, icon }) => (
          <IconButton key={title} size="small" title={title} onClick={onClick}>
            {icon}
          </IconButton>
        ))}
      </CodeActionsContainer>
      <Highlight
        prism={Prism}
        theme={colorScheme === "dark" ? DARK_PRISM_THEME : LIGHT_PRISM_THEME}
        language={language}
        code={code}
      >
        {({ style, tokens, getLineProps, getTokenProps }) => (
          <StyledPre style={style} wrap={wrapLines}>
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
