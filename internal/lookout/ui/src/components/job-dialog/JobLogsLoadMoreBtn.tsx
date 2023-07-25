import React from 'react'
import { createTheme, ThemeProvider } from '@mui/material/styles';
import Button from '@mui/material/Button';

// Augment the palette to include an ochre color
declare module '@mui/material/styles' {
  interface Palette {
    jobLogBtn: Palette['primary'];
  }

  interface PaletteOptions {
    jobLogBtn?: PaletteOptions['primary'];
  }
}

// Update the Button's color options to include an ochre option
declare module '@mui/material/Button' {
  interface ButtonPropsColorOverrides {
    jobLogBtn: true;
  }
}


type JobLogsLoadMoreBtnProps = {
  text: string,

}

const theme = createTheme({
  palette: {
    jobLogBtn: {
      main: '#00AAE1',
      contrastText: "#fff"
    },
  },
});

export default  function JobLogsLoadMoreBtn(props: JobLogsLoadMoreBtnProps) {
  return (
    <ThemeProvider theme={theme}>

    <Button variant="contained" color="jobLogBtn">
      {props?.text}
     </Button>
      </ThemeProvider>
  )
}

