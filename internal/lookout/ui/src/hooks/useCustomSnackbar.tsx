import { Close } from "@mui/icons-material"
import { IconButton } from "@mui/material"
import { useSnackbar, VariantType } from "notistack"

export const useCustomSnackbar = () => {
  const { enqueueSnackbar, closeSnackbar } = useSnackbar()
  return (message: string, variant: VariantType) => {
    enqueueSnackbar(message, {
      variant: variant,
      action: (snackbarKey) => (
        <IconButton onClick={() => closeSnackbar(snackbarKey)}>
          <Close style={{ color: "white" }} />
        </IconButton>
      ),
    })
  }
}
