import cspellPlugin from "@cspell/eslint-plugin"
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
      "@cspell": cspellPlugin,
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

      "no-console": "error",

      "@cspell/spellchecker": [
        "error",
        {
          autoFix: true,
          cspell: {
            language: "en-GB",

            ignoreWords: [
              "preempt",
              "preempts",
              "preempted",
              "preemptible",
              "preempting",
              "preemption",
              "groupable",
              "aggregatable",
              "ingester",

              // Use the American spelling of these words for consistency with the Armada API
              "reprioritize",
              "reprioritized",
              "reprioritizing",
              "reprioritization",
              "reprioritizations",
              "reprioritizable",

              // Maintainers whose names may appear in comments
              "mauriceyap",

              // URL parameter keys
              "ltta", // last transition time aggregate
            ],

            /* eslint-disable @cspell/spellchecker */
            flagWords: [
              // Use the American spelling of these words for consistency with the Armada API
              "reprioritise",
              "reprioritised",
              "reprioritising",
              "reprioritisation",
              "reprioritisations",
              "reprioritisable",
            ],
            /* eslint-disable @cspell/spellchecker */
          },
        },
      ],

      "import/order": [
        "error",
        {
          groups: ["builtin", "external", "internal", "sibling"],

          pathGroups: [
            {
              pattern: "react",
              group: "external",
              position: "before",
            },
            {
              pattern: "../../../../../../**",
              group: "internal",
              position: "after",
            },
            {
              pattern: "../../../../../**",
              group: "internal",
              position: "after",
            },
            {
              pattern: "../../../../**",
              group: "internal",
              position: "after",
            },
            {
              pattern: "../../../**",
              group: "internal",
              position: "after",
            },
            {
              pattern: "../../**",
              group: "internal",
              position: "after",
            },
            {
              pattern: "../**",
              group: "internal",
              position: "after",
            },
            {
              pattern: "./**",
              group: "sibling",
              position: "after",
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
