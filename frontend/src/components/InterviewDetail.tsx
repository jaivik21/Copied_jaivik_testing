import React, { useEffect, useState } from 'react';
import { Share2, Eye, Palette, Pencil, Users, Filter } from 'lucide-react';

interface Props { apiBaseUrl: string; interviewId: string }

export default function InterviewDetail({ apiBaseUrl, interviewId }: Props) {
  const [responses, setResponses] = useState<any[]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [interviewMeta, setInterviewMeta] = useState<any>(null);
  const [overallAnalysis, setOverallAnalysis] = useState<any>(null);
  const [loadingAnalysis, setLoadingAnalysis] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const loadData = async () => {
      try {
        const meta = await fetch(`${apiBaseUrl}/api/interview/get-interview`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            interview_id: interviewId
          }),
        });
        if (cancelled) return;
        const metaData = await meta.json();
        setInterviewMeta(metaData);
        
        const rs = await fetch(`${apiBaseUrl}/api/interview/list-interview-responses`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            interview_id: interviewId
          }),
        });
        if (cancelled) return;
        const rsData = await rs.json();
        if (rsData?.ok) {
          setResponses(rsData.responses || []);
          // Load overall analysis if there are responses
          if (rsData.responses && rsData.responses.length > 0) {
            setLoadingAnalysis(true);
            try {
              const analysis = await fetch(`${apiBaseUrl}/api/interview/get-overall-analysis`, {
                method: 'POST',
                headers: {
                  'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                  interview_id: interviewId
                }),
              });
              if (cancelled) return;
              const analysisData = await analysis.json();
              if (analysisData?.ok) setOverallAnalysis(analysisData);
            } catch (e) {
              console.error('Failed to load overall analysis', e);
            } finally {
              if (!cancelled) setLoadingAnalysis(false);
            }
          }
        }
      } catch (e: any) {
        if (!cancelled) setError(e?.message || 'Failed to load');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    loadData();
    return () => { cancelled = true; };
  }, [apiBaseUrl, interviewId]);

  if (loading) return <div className="p-6">Loading...</div>;
  if (error) return <div className="p-6 text-red-700">{error}</div>;

  const candidateLink = interviewMeta?.candidate_link 
    ? `${window.location.origin}${interviewMeta.candidate_link}` 
    : `${window.location.origin}/candidate/interview/${interviewId}`;
  const hasResponses = responses.length > 0;

  const copyLink = () => {
    navigator.clipboard.writeText(candidateLink);
    alert('Link copied to clipboard!');
  };

  return (
    <div className="min-h-screen bg-white">
      {/* Header Bar */}
      <div className="border-b bg-white sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <h1 className="text-xl font-semibold">{interviewMeta?.name || 'Interview'}</h1>
            <div className="w-2 h-2 bg-blue-500 rounded-full"></div>
          </div>
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-1 text-gray-600">
              <Users className="w-4 h-4" />
              <span>: {responses.length}</span>
            </div>
            <button onClick={copyLink} className="p-2 hover:bg-gray-100 rounded" title="Share">
              <Share2 className="w-4 h-4 text-gray-600" />
            </button>
            <button className="p-2 hover:bg-gray-100 rounded" title="View">
              <Eye className="w-4 h-4 text-gray-600" />
            </button>
            <button className="p-2 hover:bg-gray-100 rounded" title="Customize">
              <Palette className="w-4 h-4 text-gray-600" />
            </button>
            <button className="p-2 hover:bg-gray-100 rounded" title="Edit">
              <Pencil className="w-4 h-4 text-gray-600" />
            </button>
            <div className="flex items-center gap-2 ml-4">
              <span className="text-sm text-gray-600">{interviewMeta?.is_open !== false ? 'Open' : 'Closed'}</span>
              <button
                onClick={async () => {
                  try {
                    const res = await fetch(`${apiBaseUrl}/api/interview/toggle-interview-status`, {
                      method: 'POST',
                      headers: {
                        'Content-Type': 'application/json',
                      },
                      body: JSON.stringify({
                        interview_id: interviewId
                      }),
                    });
                    const data = await res.json();
                    if (data.ok) {
                      setInterviewMeta({ ...interviewMeta, is_open: data.is_open });
                    }
                  } catch (e) {
                    console.error('Failed to toggle status:', e);
                  }
                }}
                className={`relative w-11 h-6 rounded-full transition-colors ${interviewMeta?.is_open !== false ? 'bg-blue-600' : 'bg-gray-300'}`}
              >
                <span className={`absolute top-1 left-1 w-4 h-4 bg-white rounded-full transition-transform ${interviewMeta?.is_open !== false ? 'translate-x-5' : ''}`}></span>
              </button>
            </div>
          </div>
        </div>
      </div>

      <div className="flex max-w-7xl mx-auto">
        {/* Left Sidebar */}
        <div className="w-64 border-r bg-gray-50 min-h-screen">
          <div className="p-4 border-b">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Filter className="w-4 h-4 text-gray-600" />
                <span className="text-sm font-medium">Filter By</span>
              </div>
              <button className="text-gray-400 hover:text-gray-600">
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" />
                </svg>
              </button>
            </div>
          </div>
          <div className="p-4">
            {hasResponses ? (
              <div className="space-y-2">
                {overallAnalysis?.candidates?.map((candidate: any) => {
                  const date = candidate.created_at ? new Date(candidate.created_at) : null;
                  const formattedDate = date ? `${date.getDate()}-${date.getMonth() + 1}-${date.getFullYear()} ${date.getHours()}:${String(date.getMinutes()).padStart(2, '0')}` : '';
                  return (
                    <a
                      key={candidate.response_id}
                      href={`/interviews/${interviewId}/responses/${candidate.response_id}`}
                      onClick={(e) => {
                        e.preventDefault();
                        window.history.pushState({}, '', `/interviews/${interviewId}/responses/${candidate.response_id}`);
                        window.dispatchEvent(new PopStateEvent('popstate'));
                      }}
                      className="block p-3 bg-white rounded border hover:bg-gray-50 cursor-pointer"
                    >
                      <div className="font-medium text-sm">{candidate.name || 'Anonymous'}'s Response</div>
                      {formattedDate && <div className="text-xs text-gray-500 mt-1">{formattedDate}</div>}
                      <div className="flex items-center gap-2 mt-2">
                        <div className="w-8 h-8 rounded-full bg-blue-600 text-white flex items-center justify-center text-xs font-bold">
                          {candidate.overall_score || 0}
                        </div>
                      </div>
                    </a>
                  );
                })}
              </div>
            ) : (
              <div className="text-sm text-gray-500 mt-4">No responses to display</div>
            )}
          </div>
        </div>

        {/* Main Content Area */}
        <div className="flex-1">
          {hasResponses ? (
            <div className="p-6">
              {loadingAnalysis && (
                <div className="text-center py-8">Loading analysis...</div>
              )}
              {!loadingAnalysis && overallAnalysis && (
                <>
                  {/* Overall Analysis Header */}
                  <div className="mb-6">
                    <h1 className="text-2xl font-bold mb-2">Overall Analysis</h1>
                    <p className="text-gray-600 mb-4">Interviewer used: Empathetic Bob</p>
                    
                    {/* Interview Description */}
                    <div className="mb-6">
                      <h2 className="font-semibold mb-2">Interview Description</h2>
                      <p className="text-gray-700">{overallAnalysis.interview?.objective || overallAnalysis.interview?.description || 'No description available'}</p>
                    </div>
                  </div>

                  {/* Candidates Performance Table */}
                    <div className="mb-6 border rounded-lg overflow-hidden">
                      <table className="w-full">
                        <thead className="bg-gray-50 border-b">
                          <tr>
                            <th className="px-4 py-3 text-left text-sm font-semibold text-gray-700">Name</th>
                            <th className="px-4 py-3 text-left text-sm font-semibold text-gray-700 cursor-pointer hover:bg-gray-100">
                              Overall Score
                              <svg className="inline ml-1 w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" />
                              </svg>
                            </th>
                            <th className="px-4 py-3 text-left text-sm font-semibold text-gray-700 cursor-pointer hover:bg-gray-100">
                              Communication Score
                              <svg className="inline ml-1 w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" />
                              </svg>
                            </th>
                            <th className="px-4 py-3 text-left text-sm font-semibold text-gray-700">Summary</th>
                            <th className="px-4 py-3 text-left text-sm font-semibold text-gray-700">Status</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y">
                          {overallAnalysis.candidates?.map((candidate: any) => (
                            <tr key={candidate.response_id} className="hover:bg-gray-50">
                              <td className="px-4 py-3">
                                <a
                                  href={`/interviews/${interviewId}/responses/${candidate.response_id}`}
                                  onClick={(e) => {
                                    e.preventDefault();
                                    window.history.pushState({}, '', `/interviews/${interviewId}/responses/${candidate.response_id}`);
                                    window.dispatchEvent(new PopStateEvent('popstate'));
                                  }}
                                  className="text-blue-600 hover:underline flex items-center gap-1"
                                >
                                  {candidate.name || 'Anonymous'}
                                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                                  </svg>
                                </a>
                              </td>
                              <td className="px-4 py-3 font-semibold">{candidate.overall_score || 0}</td>
                              <td className="px-4 py-3">{candidate.communication_score || 0}</td>
                              <td className="px-4 py-3 text-gray-700">{candidate.summary || '-'}</td>
                              <td className="px-4 py-3">
                                <select
                                  value={candidate.status || 'no_status'}
                                  onChange={async (e) => {
                                    try {
                                      const res = await fetch(`${apiBaseUrl}/api/interview/update-response-status`, {
                                        method: 'POST',
                                        headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({ 
                                          response_id: candidate.response_id,
                                          status: e.target.value 
                                        }),
                                      });
                                      const data = await res.json();
                                      if (data.ok) {
                                        const updated = overallAnalysis.candidates.map((c: any) =>
                                          c.response_id === candidate.response_id ? { ...c, status: data.status, status_source: data.status_source } : c
                                        );
                                        setOverallAnalysis({ ...overallAnalysis, candidates: updated });
                                      }
                                    } catch (err) {
                                      console.error('Failed to update status:', err);
                                    }
                                  }}
                                  className={`px-3 py-1 rounded text-sm font-medium ${
                                    candidate.status === 'selected' || candidate.status === 'shortlisted' ? 'bg-green-100 text-green-800' :
                                    candidate.status === 'rejected' || candidate.status === 'not_selected' ? 'bg-red-100 text-red-800' :
                                    candidate.status === 'potential' ? 'bg-yellow-100 text-yellow-800' :
                                    'bg-gray-100 text-gray-800'
                                  }`}
                                >
                                  <option value="no_status">No Status</option>
                                  <option value="selected">Selected</option>
                                  <option value="shortlisted">Shortlisted</option>
                                  <option value="potential">Potential</option>
                                  <option value="rejected">Rejected</option>
                                  <option value="not_selected">Not Selected</option>
                                </select>
                                {candidate.status_source === 'auto' && (
                                  <span className="ml-2 text-xs text-gray-500">(Auto)</span>
                                )}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>

                    {/* Analytics Cards */}
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                      {/* Average Duration Card */}
                      <div className="border rounded-lg p-6">
                        <div className="text-3xl font-bold mb-2">{overallAnalysis.metrics?.average_duration || '0m 0s'}</div>
                        <div className="text-sm text-gray-600 mb-1">Average Duration</div>
                        <div className="text-sm text-gray-500">Interview Completion Rate: {overallAnalysis.metrics?.completion_rate || '0%'}</div>
                      </div>

                      {/* Candidate Sentiment Card */}
                      <div className="border rounded-lg p-6">
                        <div className="text-sm font-semibold mb-4">Candidate Sentiment</div>
                        <div className="flex items-center justify-center mb-4">
                          <div className="relative w-32 h-32">
                            <svg viewBox="0 0 100 100" className="transform -rotate-90">
                              <circle cx="50" cy="50" r="45" fill="none" stroke="#e5e7eb" strokeWidth="10" />
                              {overallAnalysis.metrics?.sentiment && (() => {
                                const positive = overallAnalysis.metrics.sentiment.positive || 0;
                                const neutral = overallAnalysis.metrics.sentiment.neutral || 0;
                                const negative = overallAnalysis.metrics.sentiment.negative || 0;
                                const total = positive + neutral + negative;
                                if (total === 0) return null;
                                const posPercent = (positive / total) * 100;
                                const neuPercent = (neutral / total) * 100;
                                let offset = 0;
                                return (
                                  <React.Fragment key="sentiment-chart">
                                    <circle cx="50" cy="50" r="45" fill="none" stroke="#10b981" strokeWidth="10" strokeDasharray={`${2 * Math.PI * 45 * posPercent / 100} ${2 * Math.PI * 45}`} strokeDashoffset={`-${offset}`} />
                                    <circle cx="50" cy="50" r="45" fill="none" stroke="#eab308" strokeWidth="10" strokeDasharray={`${2 * Math.PI * 45 * neuPercent / 100} ${2 * Math.PI * 45}`} strokeDashoffset={`-${offset + 2 * Math.PI * 45 * posPercent / 100}`} />
                                    <circle cx="50" cy="50" r="45" fill="none" stroke="#ef4444" strokeWidth="10" strokeDasharray={`${2 * Math.PI * 45 * (negative / total) * 100} ${2 * Math.PI * 45}`} strokeDashoffset={`-${offset + 2 * Math.PI * 45 * (posPercent + neuPercent) / 100}`} />
                                  </React.Fragment>
                                );
                              })()}
                            </svg>
                          </div>
                        </div>
                        <div className="space-y-1 text-xs">
                          <div className="flex items-center gap-2">
                            <div className="w-3 h-3 rounded-full bg-green-500"></div>
                            <span>Positive ({overallAnalysis.metrics?.sentiment?.positive || 0})</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="w-3 h-3 rounded-full bg-yellow-500"></div>
                            <span>Neutral ({overallAnalysis.metrics?.sentiment?.neutral || 0})</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="w-3 h-3 rounded-full bg-red-500"></div>
                            <span>Negative ({overallAnalysis.metrics?.sentiment?.negative || 0})</span>
                          </div>
                        </div>
                      </div>

                      {/* Candidate Status Card */}
                      <div className="border rounded-lg p-6">
                        <div className="text-sm font-semibold mb-4">Candidate Status</div>
                        <div className="text-lg font-bold mb-4">Total Responses: {overallAnalysis.metrics?.status?.total_responses || 0}</div>
                        <div className="space-y-2 text-xs">
                          <div className="flex items-center gap-2">
                            <div className="w-3 h-3 rounded-full bg-green-500"></div>
                            <span>Selected ({overallAnalysis.metrics?.status?.selected || 0})</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="w-3 h-3 rounded-full bg-yellow-500"></div>
                            <span>Potential ({overallAnalysis.metrics?.status?.potential || 0})</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="w-3 h-3 rounded-full bg-red-500"></div>
                            <span>Not Selected ({overallAnalysis.metrics?.status?.not_selected || 0})</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="w-3 h-3 rounded-full bg-gray-400"></div>
                            <span>No Status ({overallAnalysis.metrics?.status?.no_status || 0})</span>
                          </div>
                        </div>
                      </div>
                    </div>
                </>
              )}
              {!loadingAnalysis && !overallAnalysis && (
                <div className="text-center py-8 text-gray-500">No analysis data available</div>
              )}
            </div>
          ) : (
            <div className="flex items-center justify-center min-h-screen bg-white">
              <div className="text-center max-w-md">
                {/* Illustration */}
                <div className="mb-6 flex justify-center">
                  <svg width="200" height="200" viewBox="0 0 200 200" fill="none" xmlns="http://www.w3.org/2000/svg">
                    {/* Person with telescope */}
                    <circle cx="100" cy="80" r="25" fill="#1f2937" />
                    <path d="M100 105 L100 140 L85 155 L115 155 L100 140 Z" fill="#1f2937" />
                    <path d="M75 130 L60 130 L60 140 L75 140 Z" fill="#1f2937" />
                    <path d="M125 130 L140 130 L140 140 L125 140 Z" fill="#1f2937" />
                    {/* Telescope */}
                    <line x1="70" y1="90" x2="50" y2="70" stroke="#1f2937" strokeWidth="4" strokeLinecap="round" />
                    <circle cx="50" cy="70" r="8" fill="none" stroke="#1f2937" strokeWidth="3" />
                    {/* Thought bubbles */}
                    <circle cx="35" cy="50" r="12" fill="#e5e7eb" />
                    <circle cx="35" cy="35" r="10" fill="#e5e7eb" />
                    <circle cx="35" cy="20" r="8" fill="#e5e7eb" />
                  </svg>
                </div>
                <h2 className="text-2xl font-bold text-gray-900 mb-2">No Responses Yet</h2>
                <p className="text-gray-600">Please share with your intended respondents</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
