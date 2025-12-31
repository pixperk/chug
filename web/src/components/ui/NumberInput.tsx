import { InputHTMLAttributes, forwardRef } from 'react';
import { Plus, Minus } from 'lucide-react';

interface NumberInputProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'type' | 'onChange'> {
  label?: string;
  hint?: string;
  error?: string;
  value: string;
  onChange: (value: string) => void;
  min?: number;
  max?: number;
  step?: number;
}

export const NumberInput = forwardRef<HTMLInputElement, NumberInputProps>(
  ({ label, hint, error, value, onChange, min, max, step = 1, className = '', ...props }, ref) => {
    const handleIncrement = () => {
      const currentValue = parseInt(value) || 0;
      const newValue = currentValue + step;
      if (max === undefined || newValue <= max) {
        onChange(newValue.toString());
      }
    };

    const handleDecrement = () => {
      const currentValue = parseInt(value) || 0;
      const newValue = currentValue - step;
      if (min === undefined || newValue >= min) {
        onChange(newValue.toString());
      }
    };

    return (
      <div className="w-full">
        {label && (
          <label className="block text-sm font-medium text-gray-300 mb-2">
            {label}
          </label>
        )}
        <div className="relative flex items-center">
          <button
            type="button"
            onClick={handleDecrement}
            className="absolute left-2 z-10 p-1 rounded text-gray-400 hover:text-foreground hover:bg-gray-800 transition-colors"
            disabled={min !== undefined && (parseInt(value) || 0) <= min}
          >
            <Minus className="w-3.5 h-3.5" />
          </button>
          <input
            ref={ref}
            type="number"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            min={min}
            max={max}
            step={step}
            className={`
              w-full px-10 py-2 bg-gray-900 border border-gray-800 rounded-lg
              text-foreground text-center
              focus:outline-none focus:ring-2 focus:ring-accent/50 focus:border-accent
              transition-all duration-200
              [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none
              disabled:opacity-50 disabled:cursor-not-allowed
              ${error ? 'border-error focus:ring-error/50 focus:border-error' : ''}
              ${className}
            `}
            {...props}
          />
          <button
            type="button"
            onClick={handleIncrement}
            className="absolute right-2 z-10 p-1 rounded text-gray-400 hover:text-foreground hover:bg-gray-800 transition-colors"
            disabled={max !== undefined && (parseInt(value) || 0) >= max}
          >
            <Plus className="w-3.5 h-3.5" />
          </button>
        </div>
        {hint && !error && (
          <p className="mt-1.5 text-xs text-gray-500">{hint}</p>
        )}
        {error && (
          <p className="mt-1.5 text-xs text-error">{error}</p>
        )}
      </div>
    );
  }
);

NumberInput.displayName = 'NumberInput';
