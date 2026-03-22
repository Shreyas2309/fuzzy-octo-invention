import { useState, useRef, useEffect, useCallback } from 'react';
import { terminalRun } from '../api/client';

interface HistoryEntry {
  command: string;
  output: string;
  error: boolean;
  ts: number;
}

const WELCOME = `lsm-kv Explorer Terminal
type 'help' to list commands
`;

export default function Terminal() {
  const [input, setInput] = useState('');
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [cmdHistory, setCmdHistory] = useState<string[]>([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  const [running, setRunning] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Auto-scroll on new output
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [history]);

  // Focus input on mount and on click
  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const handleSubmit = useCallback(async () => {
    const cmd = input.trim();
    if (!cmd) return;

    setInput('');
    setCmdHistory((prev) => [cmd, ...prev]);
    setHistoryIndex(-1);

    if (cmd === 'clear') {
      setHistory([]);
      return;
    }

    setRunning(true);
    try {
      const res = await terminalRun(cmd);

      if (res.output === '__CLEAR__') {
        setHistory([]);
      } else {
        setHistory((prev) => [
          ...prev,
          { command: cmd, output: res.output, error: res.error, ts: Date.now() },
        ]);
      }
    } catch (err: any) {
      setHistory((prev) => [
        ...prev,
        { command: cmd, output: `connection error: ${err.message}`, error: true, ts: Date.now() },
      ]);
    } finally {
      setRunning(false);
      // Re-focus after command completes
      setTimeout(() => inputRef.current?.focus(), 0);
    }
  }, [input]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleSubmit();
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      if (cmdHistory.length === 0) return;
      const next = Math.min(historyIndex + 1, cmdHistory.length - 1);
      setHistoryIndex(next);
      setInput(cmdHistory[next]);
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (historyIndex <= 0) {
        setHistoryIndex(-1);
        setInput('');
      } else {
        const next = historyIndex - 1;
        setHistoryIndex(next);
        setInput(cmdHistory[next]);
      }
    } else if (e.key === 'l' && e.ctrlKey) {
      e.preventDefault();
      setHistory([]);
    }
  };

  // Color output lines based on content
  const colorize = (line: string, isError: boolean) => {
    if (isError) return 'text-accent-red';
    if (line.startsWith('OK')) return 'text-accent-green';
    if (line.startsWith('(nil)') || line.startsWith('-> NOT FOUND')) return 'text-gray-500';
    if (line.startsWith('-> FOUND')) return 'text-accent-green';
    if (line.startsWith('error:')) return 'text-accent-red';
    if (line.startsWith('trace:')) return 'text-accent-amber';
    if (line.includes('bloom=negative')) return 'text-accent-red/70';
    if (line.includes('bloom=positive')) return 'text-accent-amber/70';
    return 'text-gray-300';
  };

  return (
    <div className="flex flex-col h-[calc(100vh-7rem)]">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xl font-mono font-bold text-white">Terminal</h2>
        <button
          onClick={() => setHistory([])}
          className="px-3 py-1 text-xs font-mono text-gray-500 hover:text-gray-300 border border-navy-600 rounded"
        >
          Clear
        </button>
      </div>

      {/* Terminal body */}
      <div
        className="flex-1 bg-[#0c0c14] border border-navy-600 rounded-lg overflow-y-auto p-4 font-mono text-sm cursor-text min-h-0"
        onClick={() => inputRef.current?.focus()}
      >
        {/* Welcome message */}
        <pre className="text-accent-green/70 mb-2 text-xs">{WELCOME}</pre>

        {/* History */}
        {history.map((entry, i) => (
          <div key={i} className="mb-2">
            {/* Command line */}
            <div className="flex">
              <span className="text-accent-green font-bold mr-1">lsm-kv{'>'}</span>
              <span className="text-white">{entry.command}</span>
            </div>
            {/* Output */}
            {entry.output && (
              <pre className="ml-0 mt-0.5 whitespace-pre-wrap text-xs leading-relaxed">
                {entry.output.split('\n').map((line, j) => (
                  <div key={j} className={colorize(line, entry.error)}>
                    {line}
                  </div>
                ))}
              </pre>
            )}
          </div>
        ))}

        {/* Current input line */}
        <div className="flex items-center">
          <span className="text-accent-green font-bold mr-1">lsm-kv{'>'}</span>
          <input
            ref={inputRef}
            type="text"
            className="flex-1 bg-transparent outline-none text-white caret-accent-green font-mono text-sm"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            disabled={running}
            spellCheck={false}
            autoComplete="off"
          />
          {running && (
            <span className="text-accent-amber text-xs animate-pulse ml-2">running...</span>
          )}
        </div>

        <div ref={bottomRef} />
      </div>

      {/* Status bar */}
      <div className="mt-2 flex items-center gap-4 text-[10px] font-mono text-gray-600">
        <span>Up/Down: history</span>
        <span>Ctrl+L: clear</span>
        <span>Enter: run</span>
        <span>{cmdHistory.length} commands in history</span>
      </div>
    </div>
  );
}
