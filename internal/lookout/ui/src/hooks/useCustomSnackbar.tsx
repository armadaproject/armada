import { Close } from "@mui/icons-material"
import { IconButton } from "@mui/material"
import { OptionsObject, useSnackbar, VariantType } from "notistack"

export const useCustomSnackbar = () => {
  const { enqueueSnackbar, closeSnackbar } = useSnackbar()
  return (message: string, variant: VariantType, options?: OptionsObject) => {
    enqueueSnackbar(message, {
      variant: variant,
      ...options,
      action: (snackbarKey) => (
        <IconButton onClick={() => closeSnackbar(snackbarKey)}>
          <Close style={{ color: "white" }} />
        </IconButton>
      ),
    })
  }
}
