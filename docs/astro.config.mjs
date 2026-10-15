import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import { unified } from "@astrojs/markdown-remark";

export default defineConfig({
  site: "https://natlabrockies.github.io",
  base: "/r2x-plexos/",
  outDir: "./_build",
  markdown: { processor: unified() },
  redirects: {
  "/explanations/": "/r2x-plexos/explanation/",
  "/references/": "/r2x-plexos/reference/",
  "/tutorials/": "/r2x-plexos/tutorials/round-trip/",
  "/how-tos/": "/r2x-plexos/how-to/python/"
},
  integrations: [starlight({
    title: "r2x-plexos",
    logo: {
      light: "./src/assets/r2x-logo-full-color.svg",
      dark: "./src/assets/r2x-logo-white.svg",
      alt: "R2X",
      replacesTitle: false,
    },
    social: [{ icon: "github", label: "GitHub", href: "https://github.com/NatLabRockies/r2x-plexos" }],
    editLink: { baseUrl: "https://github.com/NatLabRockies/r2x-plexos/edit/main/docs/src/content/docs/" },
    customCss: ["./src/styles/brand.css"],
    sidebar: [
  {
    "label": "Tutorials",
    "items": [
      {
        "label": "Introduction",
        "link": "/"
      },
      {
        "label": "Parse and export",
        "link": "/tutorials/round-trip/"
      }
    ]
  },
  {
    "label": "How-to guides",
    "items": [
      {
        "label": "Installation",
        "link": "/how-to/install/"
      },
      {
        "label": "Python workflow",
        "link": "/how-to/python/"
      },
      {
        "label": "CLI pipeline",
        "link": "/how-to/pipeline/"
      }
    ]
  },
  {
    "label": "Reference / API",
    "items": [
      {
        "label": "Public API and configuration",
        "link": "/reference/"
      },
      {
        "label": "Versions",
        "link": "/reference/versions/"
      }
    ]
  },
  {
    "label": "Explanation",
    "items": [
      {
        "label": "Data flow and compatibility",
        "link": "/explanation/"
      },
      {"label": "Supplemental attributes", "link": "/explanation/supplemental-attributes/"},
      {"label": "Data model", "link": "/explanation/data-model/"}
    ]
  },
  {
    "label": "Troubleshooting",
    "items": [
      {
        "label": "Common problems",
        "link": "/troubleshooting/"
      }
    ]
  },
  {
    "label": "Changelog",
    "items": [
      {
        "label": "Release history",
        "link": "/changelog/"
      }
    ]
  },
  {
    "label": "Contributors",
    "items": [
      {
        "label": "Development",
        "link": "/contributors/"
      }
    ]
  }
]
  })],
});
