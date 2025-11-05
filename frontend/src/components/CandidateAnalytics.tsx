import { useEffect, useState } from 'react';

interface Props { apiBaseUrl: string; interviewId: string; responseId: string }

export default function CandidateAnalytics({ apiBaseUrl, interviewId, responseId }: Props) {
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const abortController = new AbortController();
    const loadData = async () => {
      try {
        const res = await fetch(`${apiBaseUrl}/api/interview/get-response`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            response_id: responseId
          }),
          signal: abortController.signal
        });
        if (abortController.signal.aborted) return;
        const data = await res.json();
        setData(data);
      } catch (e: any) {
        if (e.name === 'AbortError') return;
        setError(e?.message || 'Failed to load');
      } finally {
        if (!abortController.signal.aborted) setLoading(false);
      }
    };
    loadData();
    return () => abortController.abort();
  }, [apiBaseUrl, interviewId, responseId]);

  if (loading) return <div className="p-6">Loading...</div>;
  if (error) return <div className="p-6 text-red-700">{error}</div>;

  const generalSummary = data?.general_summary || {};
  const questionSummary = data?.question_summary || [];
  const transcript = data?.transcript || [];

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <a 
        href={`/interviews/${interviewId}`} 
        className="text-blue-600 underline text-sm mb-4 inline-block" 
        onClick={(e)=>{e.preventDefault(); window.history.pushState({}, '', `/interviews/${interviewId}`); window.dispatchEvent(new PopStateEvent('popstate'));}}
      >
        Back to Interview
      </a>
      
      <h1 className="text-2xl font-bold mb-2">Candidate Analytics</h1>
      <p className="text-gray-600 mb-6">Response ID: {responseId}</p>

      {/* Candidate Info */}
      {data?.candidate && (
        <div className="mb-6 flex items-center gap-4">
          <div className="w-12 h-12 rounded-full bg-purple-600 flex items-center justify-center text-white font-semibold text-lg">
            {(data.candidate.name || 'C')[0].toUpperCase()}
          </div>
          <div>
            <div className="font-semibold">{data.candidate.name || 'Anonymous'}</div>
            <div className="text-sm text-gray-600">{data.candidate.email || ''}</div>
          </div>
        </div>
      )}

      {/* General Summary Section */}
      <div className="mb-6 border rounded-lg p-6 bg-white">
        <h2 className="text-xl font-semibold mb-4">General Summary</h2>
        
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-4">
          {/* Overall Score */}
          <div className="flex flex-col items-center">
            <div className="text-3xl font-bold mb-2">{generalSummary.overall_score ?? '--'}</div>
            <div className="text-sm text-gray-600 mb-3">Overall Hiring Score</div>
            <div className="text-sm text-gray-700 text-center">
              {generalSummary.overall_feedback || 'No feedback available.'}
            </div>
          </div>

          {/* Communication Score */}
          <div className="flex flex-col items-center">
            <div className="text-3xl font-bold mb-2">{generalSummary.communication_score ?? '--'}/10</div>
            <div className="text-sm text-gray-600 mb-3">Communication</div>
            <div className="text-sm text-gray-700 text-center">
              {generalSummary.communication_feedback || 'No feedback available.'}
            </div>
          </div>
        </div>

        {/* User Sentiment & Call Summary */}
        <div className="mt-4 pt-4 border-t">
          <div className="mb-2">
            <span className="text-sm font-medium">User Sentiment: </span>
            <span className={`text-sm ${generalSummary.sentiment === 'positive' ? 'text-green-600' : generalSummary.sentiment === 'negative' ? 'text-red-600' : 'text-yellow-600'}`}>
              {generalSummary.sentiment ? generalSummary.sentiment.charAt(0).toUpperCase() + generalSummary.sentiment.slice(1) : 'Neutral'}
            </span>
          </div>
          {generalSummary.call_summary && (
            <div className="mt-3">
              <div className="text-sm font-medium mb-1">Call Summary:</div>
              <div className="text-sm text-gray-700 bg-gray-50 p-3 rounded">{generalSummary.call_summary}</div>
            </div>
          )}
        </div>
      </div>

      {/* Question Summary Section */}
      <div className="mb-6 border rounded-lg p-6 bg-white">
        <h2 className="text-xl font-semibold mb-4">Question Summary</h2>
        {Array.isArray(questionSummary) && questionSummary.length > 0 ? (
          <div className="space-y-4">
            {questionSummary.map((qs: any, idx: number) => (
              <div key={idx} className="border-b pb-4 last:border-b-0">
                <div className="font-medium mb-2">
                  Question {qs.question_number || idx + 1}: {qs.question}
                </div>
                <div className="text-sm">
                  <span className={`inline-block px-2 py-1 rounded text-xs font-medium mr-2 ${
                    qs.status === 'asked' ? 'bg-green-100 text-green-800' :
                    qs.status === 'not_answered' ? 'bg-yellow-100 text-yellow-800' :
                    'bg-gray-100 text-gray-800'
                  }`}>
                    {qs.status === 'asked' ? 'Asked' : qs.status === 'not_answered' ? 'Not Answered' : 'Not Asked'}
                  </span>
                  {qs.summary && qs.status === 'asked' && (
                    <div className="mt-2 text-gray-700">{qs.summary}</div>
                  )}
                  {qs.status === 'not_asked' && (
                    <div className="mt-2 text-gray-500 italic">This question was not asked during the interview.</div>
                  )}
                  {qs.status === 'not_answered' && (
                    <div className="mt-2 text-gray-500 italic">This question was asked but not answered.</div>
                  )}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-sm text-gray-500">No questions available.</div>
        )}
      </div>

      {/* Transcript Section */}
      <div className="mb-6 border rounded-lg p-6 bg-white">
        <h2 className="text-xl font-semibold mb-4">Transcript</h2>
        {Array.isArray(transcript) && transcript.length > 0 ? (
          <div className="space-y-3">
            {transcript.map((entry: any, idx: number) => (
              <div key={idx} className="text-sm">
                <span className="font-semibold text-purple-600">{entry.speaker}:</span>
                <span className="ml-2 text-gray-700">{entry.text}</span>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-sm text-gray-500">No transcript available.</div>
        )}
      </div>

      {/* Q&A Section (Optional - showing raw Q&A) */}
      <div className="border rounded-lg p-6 bg-white">
        <h2 className="text-xl font-semibold mb-4">Q&A</h2>
        {Array.isArray(data?.qa_history) && data.qa_history.length > 0 ? (
          <div className="divide-y">
            {data.qa_history.map((qa: any, idx: number) => (
              <div key={idx} className="py-3">
                <div className="text-sm text-gray-500 mb-1">Q{idx + 1}</div>
                <div className="font-medium mb-2">{qa.question}</div>
                <div className="text-gray-800 whitespace-pre-wrap">{qa.answer || 'No answer provided.'}</div>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-sm text-gray-500">No Q&A yet.</div>
        )}
      </div>
    </div>
  );
}


