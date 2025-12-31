import { HTMLAttributes, forwardRef } from 'react';

interface CardProps extends HTMLAttributes<HTMLDivElement> {
  glass?: boolean;
  hover?: boolean;
}

export const Card = forwardRef<HTMLDivElement, CardProps>(
  ({ glass = false, hover = false, children, className = '', ...props }, ref) => {
    const baseStyles = 'rounded-xl p-6 border transition-all duration-200';
    const glassStyles = glass
      ? 'bg-gray-900/30 backdrop-blur-xl border-gray-800/50'
      : 'bg-gray-900 border-gray-800';
    const hoverStyles = hover ? 'hover:border-gray-700 hover:-translate-y-0.5 hover:shadow-lg hover:shadow-accent/5' : '';

    return (
      <div
        ref={ref}
        className={`${baseStyles} ${glassStyles} ${hoverStyles} ${className}`}
        {...props}
      >
        {children}
      </div>
    );
  }
);

Card.displayName = 'Card';
