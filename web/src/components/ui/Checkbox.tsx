import { forwardRef, InputHTMLAttributes, ReactNode } from 'react';
import { Check } from 'lucide-react';

interface CheckboxProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'type'> {
  label?: ReactNode;
}

export const Checkbox = forwardRef<HTMLInputElement, CheckboxProps>(
  ({ label, className = '', checked, ...props }, ref) => {
    return (
      <label className={`inline-flex items-center gap-2 cursor-pointer group ${className}`}>
        <div className="relative flex items-center justify-center">
          <input
            ref={ref}
            type="checkbox"
            checked={checked}
            className="peer absolute opacity-0 w-4 h-4 cursor-pointer"
            {...props}
          />
          <div className={`
            w-4 h-4 rounded border-2 transition-all duration-200
            flex items-center justify-center
            ${checked
              ? 'bg-accent border-accent'
              : 'border-gray-700 bg-gray-900 group-hover:border-gray-600'
            }
            peer-focus:ring-2 peer-focus:ring-accent/50
            peer-disabled:opacity-50 peer-disabled:cursor-not-allowed
          `}>
            <Check
              className={`w-3 h-3 text-background transition-all duration-200 ${
                checked ? 'opacity-100 scale-100' : 'opacity-0 scale-50'
              }`}
              strokeWidth={3}
            />
          </div>
        </div>
        {label && (
          <span className="text-sm text-gray-300 group-hover:text-foreground transition-colors select-none">
            {label}
          </span>
        )}
      </label>
    );
  }
);

Checkbox.displayName = 'Checkbox';
