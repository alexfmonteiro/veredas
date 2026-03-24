import { useState, useRef, useEffect } from 'react';
import { postQuery } from '@/lib/api';
import type { QueryResponse } from '@/lib/api';

interface Message {
  role: 'user' | 'assistant';
  content: string;
  response?: QueryResponse;
  isLoading?: boolean;
  error?: string;
}

function TierBadge({ tier }: { tier: 'direct_lookup' | 'full_llm' }) {
  const isT1 = tier === 'direct_lookup';
  return (
    <span
      className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider border ${
        isT1
          ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30'
          : 'bg-brand-500/20 text-brand-400 border-brand-500/30'
      }`}
    >
      {isT1 ? 'Tier 1 - Direct Lookup' : 'Tier 3 - LLM'}
    </span>
  );
}

function DataCitations({ response }: { response: QueryResponse }) {
  if (response.data_points.length === 0) return null;

  return (
    <div className="mt-3 rounded-lg border border-slate-700/50 bg-slate-900/50 p-3">
      <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider mb-2">
        Data Points
      </p>
      <div className="space-y-1">
        {response.data_points.map((dp, i) => (
          <div key={i} className="flex items-center justify-between text-xs">
            <span className="text-slate-400">{dp.series}</span>
            <span className="text-slate-300 font-medium">
              {dp.value.toLocaleString('en-US', { maximumFractionDigits: 4 })}
            </span>
            <span className="text-slate-500">{dp.date}</span>
          </div>
        ))}
      </div>
      {response.sources.length > 0 && (
        <p className="text-[10px] text-slate-600 mt-2">
          Sources: {response.sources.join(', ')}
        </p>
      )}
    </div>
  );
}

export function AskPage() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [questionCount, setQuestionCount] = useState(0);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const question = input.trim();
    if (!question || isSubmitting) return;

    setInput('');
    setQuestionCount((c) => c + 1);

    const userMsg: Message = { role: 'user', content: question };
    const loadingMsg: Message = { role: 'assistant', content: '', isLoading: true };

    setMessages((prev) => {
      const updated = [...prev, userMsg, loadingMsg];
      // Keep only last 10 turns (20 messages)
      if (updated.length > 20) {
        return updated.slice(updated.length - 20);
      }
      return updated;
    });

    setIsSubmitting(true);

    try {
      const response = await postQuery(question);
      setMessages((prev) =>
        prev.map((m) =>
          m.isLoading
            ? { role: 'assistant', content: response.answer, response }
            : m,
        ),
      );
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Unknown error';
      setMessages((prev) =>
        prev.map((m) =>
          m.isLoading
            ? { role: 'assistant', content: '', error: errorMsg }
            : m,
        ),
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="min-h-[calc(100vh-3.5rem)] flex flex-col max-w-3xl mx-auto px-4 sm:px-6">
      {/* Header */}
      <header className="py-6">
        <h1 className="text-2xl font-bold text-slate-100">Ask AI</h1>
        <p className="text-sm text-slate-500 mt-1">
          Ask questions about Brazilian economic indicators. Powered by
          Claude Sonnet with tiered query routing.
        </p>
      </header>

      {/* Rate limit indicator */}
      <div className="mb-4 text-xs text-slate-500">
        {questionCount} of 20 daily questions used
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto space-y-4 pb-4">
        {messages.length === 0 && (
          <div className="text-center py-16">
            <p className="text-slate-500 text-sm mb-4">
              No questions yet. Try asking something like:
            </p>
            <div className="space-y-2">
              {[
                'What is the current SELIC rate?',
                'How has the USD/BRL exchange rate changed this year?',
                'What is the latest unemployment rate in Brazil?',
              ].map((suggestion) => (
                <button
                  key={suggestion}
                  onClick={() => setInput(suggestion)}
                  className="block mx-auto text-sm text-brand-400 hover:text-brand-300 transition-colors"
                >
                  &quot;{suggestion}&quot;
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div
            key={i}
            className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
          >
            <div
              className={`max-w-[85%] rounded-xl px-4 py-3 ${
                msg.role === 'user'
                  ? 'bg-brand-600 text-white'
                  : 'border border-slate-700/50 bg-slate-800/50 text-slate-300'
              }`}
            >
              {msg.isLoading && (
                <div className="flex items-center gap-2 text-sm text-slate-400">
                  <div className="h-2 w-2 rounded-full bg-brand-500 animate-pulse" />
                  Thinking...
                </div>
              )}

              {msg.error && (
                <p className="text-sm text-red-400">Error: {msg.error}</p>
              )}

              {!msg.isLoading && !msg.error && msg.role === 'assistant' && (
                <>
                  <p className="text-sm leading-relaxed whitespace-pre-line">
                    {msg.content}
                  </p>

                  {msg.response && (
                    <>
                      <DataCitations response={msg.response} />
                      <div className="flex items-center gap-3 mt-3">
                        <TierBadge tier={msg.response.tier_used} />
                        {msg.response.llm_tokens_used > 0 && (
                          <span className="text-[10px] text-slate-500">
                            {msg.response.llm_tokens_used.toLocaleString()} tokens
                          </span>
                        )}
                      </div>
                    </>
                  )}
                </>
              )}

              {msg.role === 'user' && (
                <p className="text-sm">{msg.content}</p>
              )}
            </div>
          </div>
        ))}

        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="sticky bottom-0 py-4 bg-slate-900/80 backdrop-blur-sm border-t border-slate-800">
        <form onSubmit={handleSubmit} className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about Brazilian economic indicators..."
            disabled={isSubmitting}
            className="flex-1 rounded-lg border border-slate-700/50 bg-slate-800/50 px-4 py-2.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-brand-500/50 focus:ring-1 focus:ring-brand-500/50 disabled:opacity-50"
          />
          <button
            type="submit"
            disabled={isSubmitting || !input.trim()}
            className="rounded-lg bg-brand-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-brand-500 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Send
          </button>
        </form>

        <p className="text-[10px] text-slate-600 mt-2 text-center">
          AI-generated responses may be inaccurate. Verify data with official sources (BCB, IBGE, Tesouro Nacional).
        </p>
      </div>
    </div>
  );
}
