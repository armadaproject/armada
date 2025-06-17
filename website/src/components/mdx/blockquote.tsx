/**
 * Custom blockquote component for MDX content. Designed to add support for GitHub-style alerts.
 */
import React, { forwardRef, type HTMLAttributes, type ReactNode } from 'react';
import {
  Info,
  Lightbulb,
  MessageSquareWarning,
  TriangleAlert,
  OctagonAlert,
} from 'lucide-react';
import { cva } from 'class-variance-authority';
import asTitle from 'title';
import { cn } from '@/utils/cn';

type BlockquoteType =
  | 'default'
  | 'note'
  | 'tip'
  | 'important'
  | 'warning'
  | 'caution';

type BlockquoteProps = Omit<
  HTMLAttributes<HTMLDivElement>,
  'title' | 'type' | 'icon'
> & {
  /**
   * Force a title
   */
  title?: ReactNode;

  /**
   * @defaultValue default
   */
  type?: BlockquoteType;

  /**
   * Force an icon
   */
  icon?: ReactNode;
};

const blockquoteVariants = cva(
  'my-4 flex gap-2 rounded-lg border border-s-5 bg-fd-card p-3 text-sm text-fd-card-foreground shadow-md',
  {
    variants: {
      type: {
        default: 'border-s-gray-500/50',
        note: 'border-s-blue-500/50',
        tip: 'border-s-green-500/50',
        important: 'border-s-purple-500/50',
        warning: 'border-s-amber-500/50',
        caution: 'border-s-red-500/50',
      },
    },
  }
);

const icons: Record<BlockquoteType, ReactNode | null> = {
  default: null,
  note: <Info className='size-5 text-blue-500' />,
  tip: <Lightbulb className='size-5 text-green-500' />,
  important: <MessageSquareWarning className='size-5 text-purple-500' />,
  warning: <TriangleAlert className='size-5 text-amber-500' />,
  caution: <OctagonAlert className='size-5 text-red-500' />,
};

function getAlertTitle(type: BlockquoteType): ReactNode | null {
  if (!type || type === 'default') return null;
  return <p className='font-medium !my-0'>{asTitle(type)}</p>;
}

export const Blockquote = forwardRef<HTMLDivElement, BlockquoteProps>(
  ({ className, children, title, type = 'default', icon, ...props }, ref) => {
    if (!type) type = 'default';
    if (!icon) icon = icons[type];
    if (!title) title = getAlertTitle(type);
    return (
      <div
        ref={ref}
        className={cn(
          blockquoteVariants({
            type: type,
          }),
          className
        )}
        {...props}
      >
        {icon}
        <div className='min-w-0 flex flex-col gap-2 flex-1'>
          {title}
          <div className='text-fd-muted-foreground prose-no-margin empty:hidden'>
            {children}
          </div>
        </div>
      </div>
    );
  }
);

Blockquote.displayName = 'Blockquote';
