import { defineConfig } from "astro/config";
import mermaid from "astro-mermaid";
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
  integrations: [
    mermaid({ enableLog: false, mermaidConfig: { theme: "base", fontFamily: "system-ui, sans-serif", flowchart: { curve: "basis", nodeSpacing: 35, rankSpacing: 45, padding: 16 } } }),
    starlight({
    title: "r2x-plexos",
    routeMiddleware: "./src/routeData.ts",
    logo: {
      light: "./src/assets/r2x-logo-full-color.svg",
      dark: "./src/assets/r2x-logo-white.svg",
      alt: "R2X",
      replacesTitle: false,
    },
    social: [{ icon: "github", label: "GitHub", href: "https://github.com/NatLabRockies/r2x-plexos" }],
    editLink: { baseUrl: "https://github.com/NatLabRockies/r2x-plexos/edit/main/docs/src/content/docs/" },
    customCss: ["./src/styles/brand.css", "./src/styles/api.css"],
    sidebar: [
  {
    "label": "Getting started",
    "items": [
      {"label": "Plugins and workflow", "link": "/getting-started/plugins/"},
      {
        "label": "Installation",
        "link": "/getting-started/installation/"
      },
      {
        "label": "Environment and prerequisites",
        "link": "/getting-started/environment/"
      },
      {
        "label": "Verify installation and first use",
        "link": "/getting-started/verify/"
      }
    ]
  },
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
        "label": "Python workflow",
        "link": "/how-to/python/"
      },
      {
        "label": "Inspect an archive",
        "link": "/how-to/inspect-system/"
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
      {"label": "Python API", "link": "/reference/api/"},
      {
        "label": "Configuration and lifecycle",
        "link": "/reference/"
      }
    ]
  },
  {
    "label": "Explanation",
    "items": [
      {
        "label": "PLEXOS objects and relationships",
        "link": "/explanation/"
      },
      {
        "label": "Supplemental attributes",
        "link": "/explanation/supplemental-attributes/"
      },
      {
        "label": "Data model",
        "link": "/explanation/data-model/"
      }
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
    "link": "/changelog/"
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
