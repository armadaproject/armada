import eslint from "@eslint/js"
import queryPlugin from "@tanstack/eslint-plugin-query"
import tsParser from "@typescript-eslint/parser"
import eslintConfigPrettier from "eslint-config-prettier/flat"
import importPlugin from "eslint-plugin-import"
import prettierRecommended from "eslint-plugin-prettier/recommended"
import reactPlugin from "eslint-plugin-react"
import tseslint from "typescript-eslint"

export default tseslint.config(
  {
    ignores: ["build", "src/openapi/"],
  },

  {
    files: ["src/**/*.{js,ts,tsx}", "*.config.mjs"],

    extends: [
      eslint.configs.recommended,
      tseslint.configs.recommended,
      reactPlugin.configs.flat.recommended,
      queryPlugin.configs["flat/recommended"],
      eslintConfigPrettier,
      prettierRecommended,
    ],

    plugins: {
      import: importPlugin,
    },

    languageOptions: {
      parser: tsParser,
      ecmaVersion: 2020,
      sourceType: "module",

      parserOptions: {
        ecmaFeatures: {
          jsx: true,
        },
      },
    },

    settings: {
      react: {
        version: "detect",
      },
    },

    rules: {
      "prettier/prettier": [
        "error",
        {
          endOfLine: "auto",
        },
      ],

      "import/order": [
        "error",
        {
          groups: ["builtin", "external", "internal"],

          pathGroups: [
            {
              pattern: "react",
              group: "external",
              position: "before",
            },
          ],

          pathGroupsExcludedImportTypes: ["react"],
          "newlines-between": "always",

          alphabetize: {
            order: "asc",
            caseInsensitive: false,
          },
        },
      ],

      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
        },
      ],

      "@typescript-eslint/no-empty-function": "error",
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/explicit-module-boundary-types": "off",
      "react/display-name": "off",
      "react/react-in-jsx-scope": "off",
      "react/prop-types": "off",
    },
  },
)
