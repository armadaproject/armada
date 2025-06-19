---
title: 'GitHub Markdown Cheatsheet'
---

GitHub uses its own Markdown processor, which is a variant of the CommonMark specification. This means that while it
supports most of the standard Markdown syntax, there are some GitHub-specific extensions and features.
You can use Markdown syntax, along with some additional HTML tags, to format your writing on GitHub, in places like
repository READMEs and comments on pull requests and issues.

See the [GitHub Flavored Markdown Spec](https://github.github.com/gfm/) for more examples.

See [Writing on GitHub](https://docs.github.com/en/get-started/writing-on-github) for more details.

## Headings ✅

To create a heading, add one to six # symbols before your heading text. The number of # you use
will determine the hierarchy level and typeface size of the heading.

###### This is the smallest heading

## Styling text ✅

**This text is bold**

_This text is italic_

~~This text is strikethrough~~

**This text is bold with _nested italic_**

**_This text is bold and italic_**

This text is <sub>subscript</sub>

This text is <sup>superscript</sup>

This text is <ins>underlined</ins>

This is a <kbd>keyboard shortcut</kbd>

## Quoting text ✅

> This is a blockquote.

## Empty Quotes ❌

The blockquote exists but empty, so nothing would be displayed.

>

## Quoting code ✅

This is a `inline code` example.

```python
print("This is a Python3 code block")
```

## Supported color models ❌

In issues, pull requests, and discussions, you can call out colors within a sentence by using backticks. A supported
color model within backticks will display a visualization of the color.

The background color is `#ffffff` for light mode and `#000000` for dark mode, also `rgb(9, 105, 218)`.

## Links ✅

This site was built using [GitHub Pages](https://pages.github.com/).

## Section links ✅

You can link to a section of a page by using the section's heading text, with spaces replaced by hyphens and all
lowercase.

Link to the previous section: [Links](#links-).

## Relative links ✅

Link to a file in the same repository: [introduction.md](introduction.md).

## Custom anchors ✅

Some body text of this section.

<a name="my-custom-anchor-point"></a>
Some text I want to provide a direct link to, but which doesn't have its own heading.

(… more content…)

[A link to that custom anchor](#my-custom-anchor-point)

## Line breaks ✅

To create a line break, end a line with two or more spaces, and then type return. ✅

Line one.

Line two.

or use the HTML `<br>` tag. ✅

Line one.<br/>Line two.

## Images ✅

### Hosted images

![This is a hosted image](https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png)

### Local images

![This is a local image](./assets/GitHub-Mark.png)

## Lists ✅

You can make an unordered list by preceding one or more lines of text with -, \*, or +.

- George Washington

* John Adams

- Thomas Jefferson

To order your list, precede each line with a number.

1. James Madison
2. James Monroe
3. John Quincy Adams

## Nested Lists ✅

1. First list item
   - First nested list item
     - Second nested list item

## Task lists ✅

- [x] #739
- [ ] https://github.com/octo-org/octo-repo/issues/740
- [ ] Add delight to the experience when all tasks are complete :tada:
- [ ] \(Optional) Open a followup issue

## Mentioning people and teams ❌

You can mention people and teams by using @ followed by their username or team name.

@github What do you think about these updates?

## Referencing issues and pull requests ❌

You can bring up a list of suggested issues and pull requests within the repository by typing #. Type the issue or pull
request number or title to filter the list, and then press either tab or enter to complete the highlighted result.

## Using emojis ✅

You can add emoji to your writing by typing :EMOJICODE:, a colon followed by the name of the emoji.

@octocat :+1: This PR looks great - it's ready to merge! :shipit:

For a full list of available emoji and codes, see
the [Emoji-Cheat-Sheet](https://github.com/ikatyang/emoji-cheat-sheet/blob/master/README.md).

## Footnotes ✅

Here is a simple footnote[^1].

A footnote can also have multiple lines[^2].

[^1]: My reference.

[^2]:
    To add line breaks within a footnote, prefix new lines with 2 spaces.
    This is a second line.

## Alerts ✅

Alerts are a Markdown extension based on the blockquote syntax that you can use to emphasize critical information. On
GitHub, they are displayed with distinctive colors and icons to indicate the significance of the content.

Use alerts only when they are crucial for user success and limit them to one or two per article to prevent overloading
the reader. Additionally, you should avoid placing alerts consecutively. Alerts cannot be nested within other elements.

To add an alert, use a special blockquote line specifying the alert type, followed by the alert information in a
standard blockquote. Five types of alerts are available:

> [!NOTE]
> Useful information that users should know, even when skimming content.

> [!TIP]
> Helpful advice for doing things better or more easily.

> [!IMPORTANT]
> Key information users need to know to achieve their goal.

> [!WARNING]
> Urgent info that needs immediate user attention to avoid problems.

> [!CAUTION]
>
> Advises about risks or negative outcomes of certain actions.

## Empty alerts ✅

> [!NOTE]

Displayed as a blockquote, but the alert type is not recognized.

## Wrong alerts ✅

> [!CAUTION] this shouldn't be a caution alert
> Advises about risks or negative outcomes of certain actions.

Displayed as a blockquote, but the alert type is not recognized.

## Hiding content with comments ✅

Following content won't appear in the rendered Markdown:

<!-- This content will not appear in the rendered Markdown -->

## Ignoring Markdown formatting ✅

You can tell GitHub to ignore (or escape) Markdown formatting by using \ before the Markdown character.

Let's rename \*our-new-project\* to \*our-old-project\*.

## Adding an image to suit your visitors ✅

### Example of a responsive image

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://user-images.githubusercontent.com/25423296/163456776-7f95b81a-f1ed-45f7-b7ab-8fa810d529fa.png">
  <source media="(prefers-color-scheme: light)" srcset="https://user-images.githubusercontent.com/25423296/163456779-a8556205-d0a5-45e2-ac17-42d089e3c3f8.png">
  <img alt="Shows an illustrated sun in light mode and a moon with stars in dark mode." src="https://user-images.githubusercontent.com/25423296/163456779-a8556205-d0a5-45e2-ac17-42d089e3c3f8.png">
</picture>

## Adding a table ✅

Hi, I'm Mona. You might recognize me as GitHub's mascot.

| Rank | Languages  |
| ---: | ---------- |
|    1 | JavaScript |
|    2 | Python     |
|    3 | SQL        |

## Adding a collapsed section ✅

<details>
<summary>My top languages</summary>

| Rank | Languages  |
| ---: | ---------- |
|    1 | JavaScript |
|    2 | Python     |
|    3 | SQL        |

</details>

## Adding a divider ✅

---

## Adding a video ✅

We can't use an `<iframe>` tag to embed a video, but we can link to a video on YouTube or another site and display a
thumbnail image that links to the video.

[![Watch the video](https://img.youtube.com/vi/T-D1KVIuvjA/maxresdefault.jpg)](https://youtu.be/T-D1KVIuvjA)

## Adding a quote ✅

### Example of a quote

> If we pull together and commit ourselves, then we can push through anything.

— Mona the Octocat

## Adding a comment ✅

[//]: # '<!-- TO DO: add more details about me later -->'

# Work with advanced formatting

## Creating a table ✅

| Left-aligned | Center-aligned | Right-aligned |
| :----------- | :------------: | ------------: |
| git status   |   git status   |    git status |
| git diff     |    git diff    |      git diff |
| Pipe         |       \|       |               |

| Command      | Description                                        |
| ------------ | -------------------------------------------------- |
| `git status` | List all _new or modified_ files                   |
| `git diff`   | Show file differences that **haven't been** staged |

## Creating a collapsed section ✅

<details open>

<summary>Tips for collapsed sections</summary>

### You can add a header

You can add text within a collapsed section.

You can add an image or a code block, too.

```ruby
   puts "Hello World"
```

</details>

## Creating diagrams ✅

You can create diagrams in Markdown using four different syntaxes: mermaid, geoJSON, topoJSON, and ASCII STL. Diagram
rendering is available in GitHub Issues, GitHub Discussions, pull requests, wikis, and Markdown files.

Here is a simple flow chart:

```mermaid
graph TD;
    A-->B;
    A-->C;
    B-->D;
    C-->D;
```

```mermaid
  info
```

```
geojson
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "id": 1,
      "properties": {
        "ID": 0
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
              [-90,35],
              [-90,30],
              [-85,30],
              [-85,35],
              [-90,35]
          ]
        ]
      }
    }
  ]
}
```

```
topojson
{
  "type": "Topology",
  "transform": {
    "scale": [0.0005000500050005, 0.00010001000100010001],
    "translate": [100, 0]
  },
  "objects": {
    "example": {
      "type": "GeometryCollection",
      "geometries": [
        {
          "type": "Point",
          "properties": {"prop0": "value0"},
          "coordinates": [4000, 5000]
        },
        {
          "type": "LineString",
          "properties": {"prop0": "value0", "prop1": 0},
          "arcs": [0]
        },
        {
          "type": "Polygon",
          "properties": {"prop0": "value0",
            "prop1": {"this": "that"}
          },
          "arcs": [[1]]
        }
      ]
    }
  },
  "arcs": [[[4000, 0], [1999, 9999], [2000, -9999], [2000, 9999]],[[0, 0], [0, 9999], [2000, 0], [0, -9999], [-2000, 0]]]
}
```

```
stl
solid cube_corner
  facet normal 0.0 -1.0 0.0
    outer loop
      vertex 0.0 0.0 0.0
      vertex 1.0 0.0 0.0
      vertex 0.0 0.0 1.0
    endloop
  endfacet
  facet normal 0.0 0.0 -1.0
    outer loop
      vertex 0.0 0.0 0.0
      vertex 0.0 1.0 0.0
      vertex 1.0 0.0 0.0
    endloop
  endfacet
  facet normal -1.0 0.0 0.0
    outer loop
      vertex 0.0 0.0 0.0
      vertex 0.0 0.0 1.0
      vertex 0.0 1.0 0.0
    endloop
  endfacet
  facet normal 0.577 0.577 0.577
    outer loop
      vertex 1.0 0.0 0.0
      vertex 0.0 1.0 0.0
      vertex 0.0 0.0 1.0
    endloop
  endfacet
endsolid
```

## Writing mathematical expressions ✅

This sentence uses `$` delimiters to show math inline: $\sqrt{3x-1}+(1+x)^2$

This sentence uses $\` and \`$ delimiters to show math inline: $`\sqrt{3x-1}+(1+x)^2`$

**The Cauchy-Schwarz Inequality**\
$$\left( \sum_{k=1}^n a_k b_k \right)^2 \leq \left( \sum_{k=1}^n a_k^2 \right) \left( \sum_{k=1}^n b_k^2 \right)$$

**The Cauchy-Schwarz Inequality**

```math
\left( \sum_{k=1}^n a_k b_k \right)^2 \leq \left( \sum_{k=1}^n a_k^2 \right) \left( \sum_{k=1}^n b_k^2 \right)
```

To split <span>$</span>100 in half, we calculate $100/2$

## Auto-linked references and URLs ✅

Visit https://github.com

## iframe tag ❌

The `<iframe>` tag is not supported in GitHub Markdown. You can link to an external site instead.

<iframe width="100%" style={{"aspect-ratio": "16 / 9"}}
src="https://www.youtube.com/embed/T-D1KVIuvjA?si=VdBgta3ALERtul4u" title="YouTube
video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope;
picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

Or as generated from YouTube:

<iframe width="560" height="315" src="https://www.youtube.com/embed/T-D1KVIuvjA?si=VdBgta3ALERtul4u" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

## Abbreviation ❌

\*[HTML]: Hyper Text Markup Language
The HTML specification is maintained by the W3C.

## Admonition syntax ❌

Supported on Docusaurus, but not on GitHub.

:::warning
Hello World
:::
