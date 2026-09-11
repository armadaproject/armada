import { Close } from "@mui/icons-material"
import { Button, IconButton } from "@mui/material"
import { OptionsObject, useSnackbar, VariantType } from "notistack"

export type OpenSnackbarFn = (message: string, variant: VariantType, options?: OptionsObject) => void

export type OpenUndoableSnackbarFn = (message: string, onUndo: () => void, options?: OptionsObject) => void

export const useCustomSnackbar = (): OpenSnackbarFn => {
  const { enqueueSnackbar, closeSnackbar } = useSnackbar()
  return (message: string, variant: VariantType, options?: OptionsObject) => {
    enqueueSnackbar(message, {
      variant: variant,
      ...options,
      action: (snackbarKey) => (
        <IconButton style={{ flex: "0" }} onClick={() => closeSnackbar(snackbarKey)}>
          <Close style={{ color: "white" }} />
        </IconButton>
      ),
    })
  }
}

export const useUndoableSnackbar = (): OpenUndoableSnackbarFn => {
  const { enqueueSnackbar, closeSnackbar } = useSnackbar()
  return (message: string, onUndo: () => void, options?: OptionsObject) => {
    enqueueSnackbar(message, {
      variant: "info",
      ...options,
      action: (snackbarKey) => (
        <>
          <Button
            style={{ color: "white" }}
            onClick={() => {
              onUndo()
              closeSnackbar(snackbarKey)
            }}
          >
            Undo
          </Button>
          <IconButton style={{ flex: "0" }} onClick={() => closeSnackbar(snackbarKey)}>
            <Close style={{ color: "white" }} />
          </IconButton>
        </>
      ),
    })
  }
}
