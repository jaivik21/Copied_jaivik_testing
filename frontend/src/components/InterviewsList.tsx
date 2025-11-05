import { useEffect, useState } from 'react';
import { Trash2 } from 'lucide-react';

interface Props { apiBaseUrl: string }

export default function InterviewsList({ apiBaseUrl }: Props) {
  const [items, setItems] = useState<any[]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [deletingId, setDeletingId] = useState<string | null>(null);

  const loadInterviews = async (abortSignal?: AbortSignal) => {
    try {
      const res = await fetch(`${apiBaseUrl}/api/interview/list-interviews`, {
        signal: abortSignal
      });
      if (abortSignal?.aborted) return;
      const data = await res.json();
      if (data?.ok) {
        const interviews = data.interviews || [];
        setItems(interviews);
      }
    } catch (e: any) {
      if (e.name === 'AbortError') return;
      setError(e?.message || 'Failed to load');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    const abortController = new AbortController();
    loadInterviews(abortController.signal);
    return () => abortController.abort();
  }, [apiBaseUrl]);

  const handleDelete = async (interviewId: string, e: React.MouseEvent) => {
    e.stopPropagation(); // Prevent card click
    if (!window.confirm('Are you sure you want to delete this interview? This action cannot be undone.')) {
      return;
    }

    setDeletingId(interviewId);
    try {
      const res = await fetch(`${apiBaseUrl}/api/interview/delete-interview`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          interview_id: interviewId
        }),
      });
      
      if (!res.ok) {
        const errorText = await res.text();
        throw new Error(errorText || 'Failed to delete interview');
      }

      // Reload interviews list
      await loadInterviews();
    } catch (e: any) {
      setError(e?.message || 'Failed to delete interview');
      alert(e?.message || 'Failed to delete interview');
    } finally {
      setDeletingId(null);
    }
  };

  const go = (url: string) => {
    window.history.pushState({}, '', url);
    window.dispatchEvent(new PopStateEvent('popstate'));
  };

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">My Interviews</h1>
        <button onClick={() => go('/create')} className="px-4 py-2 bg-blue-600 text-white rounded-lg">Create Interview</button>
      </div>

      {loading && <div>Loading...</div>}
      {error && <div className="text-red-700">{error}</div>}

      <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4">
        {items.map((it) => (
          <div key={it.id} className="rounded-lg border p-4 bg-indigo-50 cursor-pointer hover:bg-indigo-100 transition-colors relative" onClick={() => go(`/interviews/${it.id}`)}>
            <button
              onClick={(e) => handleDelete(it.id, e)}
              disabled={deletingId === it.id}
              className="absolute top-2 right-2 p-1.5 text-red-600 hover:bg-red-100 rounded transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              title="Delete interview"
            >
              <Trash2 className="w-4 h-4" />
            </button>
            <div className="text-lg font-semibold mb-2 pr-8">{it.name || 'Untitled'}</div>
            <div className="flex items-center gap-2 mb-1">
              <span className={`text-xs px-2 py-1 rounded ${it.is_open !== false ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}`}>
                {it.is_open !== false ? 'Open' : 'Closed'}
              </span>
            </div>
            <div className="text-sm text-gray-700">Mode: {it.mode}</div>
            <div className="text-sm text-gray-700">Questions: {it.question_count ?? '-'}</div>
            <div className="text-sm text-gray-900 font-medium mt-2">Responses: {it.responses_count}</div>
            <div className="mt-3 flex items-center gap-3">
              <button className="text-blue-700 underline text-sm hover:text-blue-900" onClick={(e)=>{e.stopPropagation(); navigator.clipboard.writeText(`${window.location.origin}/candidate/interview/${it.id}`)}}>
                Copy candidate link
              </button>
            </div>
          </div>
        ))}
        {(!loading && items.length === 0) && (
          <div className="text-gray-600">No interviews yet. Click Create Interview to add one.</div>
        )}
      </div>
    </div>
  );
}


