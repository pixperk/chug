import { HTMLAttributes, forwardRef } from 'react';

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  variant?: 'default' | 'success' | 'error' | 'warning' | 'info';
  pulse?: boolean;
}

export const Badge = forwardRef<HTMLSpanElement, BadgeProps>(
  ({ variant = 'default', pulse = false, children, className = '', ...props }, ref) => {
    const variants = {
      default: 'bg-gray-800 text-gray-300 border-gray-700',
      success: 'bg-success/10 text-success border-success/20',
      error: 'bg-error/10 text-error border-error/20',
      warning: 'bg-warning/10 text-warning border-warning/20',
      info: 'bg-accent/10 text-accent border-accent/20',
    };

    return (
      <span
        ref={ref}
        className={`
          inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded-md border
          transition-colors duration-200
          ${variants[variant]}
          ${className}
        `}
        {...props}
      >
        {pulse && (
          <span className="relative flex h-2 w-2">
            <span className={`animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${variant === 'success' ? 'bg-success' : variant === 'error' ? 'bg-error' : 'bg-accent'}`}></span>
            <span className={`relative inline-flex rounded-full h-2 w-2 ${variant === 'success' ? 'bg-success' : variant === 'error' ? 'bg-error' : 'bg-accent'}`}></span>
          </span>
        )}
        {children}
      </span>
    );
  }
);

Badge.displayName = 'Badge';
