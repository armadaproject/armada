import type { Root, Blockquote, Paragraph } from 'mdast';
import type { Plugin } from 'unified';
import { visit } from 'unist-util-visit';

const ALERT_TYPES = ['note', 'tip', 'important', 'warning', 'caution'] as const;
// const ALERT_REGEX = /^\[!(NOTE|TIP|IMPORTANT|WARNING|CAUTION)]/i;
const ALERT_REGEX = new RegExp(
  `^\\[!(${ALERT_TYPES.map((v) => v.toUpperCase()).join('|')})]`,
  'i'
);
type AlertType = (typeof ALERT_TYPES)[number];

interface RemarkGitHubAlertOptions {
  /**
   * If true, blockquotes without alert syntax will be converted to default alerts.
   * If false, only blockquotes with valid alert syntax will be transformed.
   * Default is false.
   */
  overrideDefaultStyle?: boolean;
}

interface AlertMatch {
  type: AlertType;
  remainingText: string;
}

/**
 * A remark plugin that transforms GitHub-style alert blockquotes into custom Alert components.
 *
 * Transforms blockquotes like:
 * > [!NOTE]
 * > This is a note
 *
 * Into Alert components with appropriate type properties.
 */
export const remarkGitHubAlert: Plugin<[RemarkGitHubAlertOptions?], Root> = ({
  overrideDefaultStyle = false,
} = {}) => {
  return (tree: Root) => {
    visit(tree, 'blockquote', (node: Blockquote, index, parent) => {
      const transformedNode = transformBlockquoteToAlert(
        node,
        overrideDefaultStyle
      ); // null if no transformation performed

      if (transformedNode && parent && typeof index === 'number') {
        parent.children[index] = transformedNode; // Replace the visited node with the transformed one
      }
    });
  };
};

/**
 * Transforms a blockquote node to an Alert node if it matches alert syntax
 */
function transformBlockquoteToAlert(
  node: Blockquote,
  overrideDefaultStyle: boolean
): Blockquote | null {
  let alertMatch: AlertMatch | null = null;
  const firstParagraph = findFirstParagraph(node.children);

  if (firstParagraph) {
    alertMatch = extractAlertMatch(firstParagraph);
  }

  if (alertMatch) {
    return createAlertFromMatch(node, alertMatch);
  }

  return overrideDefaultStyle ? createDefaultAlert(node) : null; // null if no transformation is needed
}

/**
 * Finds the first paragraph in the blockquote children
 */
function findFirstParagraph(
  children: Blockquote['children']
): Paragraph | null {
  return (
    children.find((child): child is Paragraph => child.type === 'paragraph') ||
    null
  );
}

/**
 * Extracts alert type and remaining text from a paragraph's first text node
 */
function extractAlertMatch(paragraph: Paragraph): AlertMatch | null {
  const firstChild = paragraph.children[0];

  if (firstChild?.type !== 'text') {
    return null;
  }

  const match = firstChild.value.match(ALERT_REGEX);

  if (!match) {
    return null;
  }

  const type = match[1].toLowerCase() as AlertType;
  const remainingText = firstChild.value
    .replace(ALERT_REGEX, '')
    .replace(/^\n+/, '');

  return { type, remainingText };
}

/**
 * Creates an Alert node from a blockquote and alert match
 */
function createAlertFromMatch(
  node: Blockquote,
  alertMatch: AlertMatch
): Blockquote {
  const updatedChildren = updateChildrenWithAlertMatch(
    node.children,
    alertMatch
  );

  return {
    ...node,
    data: {
      ...node.data,
      hName: 'Alert',
      hProperties: {
        ...node.data?.hProperties,
        type: alertMatch.type,
      },
    },
    children: updatedChildren,
  };
}

/**
 * Creates a default Alert node from a blockquote
 */
function createDefaultAlert(node: Blockquote): Blockquote {
  return {
    ...node,
    data: {
      ...node.data,
      hName: 'Alert',
      hProperties: {
        ...node.data?.hProperties,
        type: 'default',
      },
    },
  };
}

/**
 * Updates blockquote children by removing alert syntax from the first paragraph
 */
function updateChildrenWithAlertMatch(
  children: Blockquote['children'],
  alertMatch: AlertMatch
): Blockquote['children'] {
  return children.map((child, index) => {
    if (index === 0 && child.type === 'paragraph') {
      return updateFirstParagraph(child, alertMatch.remainingText);
    }
    return child;
  });
}

/**
 * Updates the first paragraph by removing alert syntax and cleaning up the content
 */
function updateFirstParagraph(
  paragraph: Paragraph,
  remainingText: string
): Paragraph {
  const [firstChild, ...restChildren] = paragraph.children;

  if (firstChild?.type !== 'text') {
    return paragraph;
  }

  // If there's remaining text, update the first text node
  if (remainingText.trim()) {
    return {
      ...paragraph,
      children: [{ ...firstChild, value: remainingText }, ...restChildren],
    };
  }

  // If no remaining text, remove the first text node and optional break
  const childrenWithoutAlert = restChildren.filter(
    (child, index) => !(index === 0 && child.type === 'break')
  );

  return {
    ...paragraph,
    children: childrenWithoutAlert,
  };
}

export default remarkGitHubAlert;
