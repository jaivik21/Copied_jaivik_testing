import { useState, useEffect } from 'react';
import InterviewSession from './InterviewSession';

interface Props { apiBaseUrl: string; interviewId: string }

export default function CandidateStart({ apiBaseUrl, interviewId }: Props) {
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [started, setStarted] = useState(false);
  const [isOpen, setIsOpen] = useState<boolean | null>(null);
  const [loadingStatus, setLoadingStatus] = useState(true);
  const [statusError, setStatusError] = useState('');
  const [accessError, setAccessError] = useState('');
  const [actualInterviewId, setActualInterviewId] = useState<string>('');

  useEffect(() => {
    const checkInterviewStatus = async () => {
      try {
        const res = await fetch(`${apiBaseUrl}/api/interview/get-interview`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            interview_id: interviewId
          }),
        });
        if (res.ok) {
          const data = await res.json();
          setIsOpen(data.is_open !== false);
          setActualInterviewId(data.id);
        } else {
          setStatusError('Failed to load interview details');
        }
      } catch (err) {
        setStatusError('Failed to check interview status');
      } finally {
        setLoadingStatus(false);
      }
    };
    
    checkInterviewStatus();
  }, [apiBaseUrl, interviewId]);

  const handleStart = async () => {
    if (!name.trim() || !email.trim()) {
      setAccessError('Please provide both name and email');
      return;
    }
    
    setAccessError('');
    setLoadingStatus(true);
    
    try {
      const res = await fetch(`${apiBaseUrl}/api/interview/start-interview`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          interview_id: actualInterviewId || interviewId,
          candidate_name: name.trim(),
          candidate_email: email.trim(),
        }),
      });
      
      const data = await res.json();
      
      if (!res.ok) {
        setAccessError(data.detail || 'Failed to start interview. Please contact HR if you believe this is an error.');
        setLoadingStatus(false);
        return;
      }
      
      setStarted(true);
    } catch (err) {
      setAccessError('Network error. Please try again.');
      setLoadingStatus(false);
    }
  };

  if (started) {
    return (
      <InterviewSession
        apiBaseUrl={apiBaseUrl}
        interviewId={actualInterviewId || interviewId}
        candidateName={name || 'Anonymous'}
        candidateEmail={email || 'anonymous@example.com'}
      />
    );
  }

  if (loadingStatus) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-orange-50 to-amber-50 flex items-center justify-center p-6">
        <div className="bg-white rounded-2xl shadow-xl max-w-md w-full p-8 text-center">
          <div className="text-gray-600">Loading interview details...</div>
        </div>
      </div>
    );
  }

  if (statusError || isOpen === false) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-orange-50 to-amber-50 flex items-center justify-center p-6">
        <div className="bg-white rounded-2xl shadow-xl max-w-md w-full p-8">
          <div className="text-center">
            <div className="text-6xl mb-4">🔒</div>
            <h1 className="text-2xl font-bold mb-2 text-red-600">Interview Closed</h1>
            <p className="text-gray-600 mb-4">
              {statusError || 'This interview is currently closed and not accepting new candidates.'}
            </p>
            <p className="text-sm text-gray-500">
              Please contact the HR team if you have any questions.
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-orange-50 to-amber-50 flex items-center justify-center p-6">
      <div className="bg-white rounded-2xl shadow-xl max-w-md w-full p-8">
        <h1 className="text-2xl font-bold mb-1">Start Interview</h1>
        <p className="text-gray-600 mb-6">Interview ID: {interviewId}</p>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">Name</label>
            <input value={name} onChange={e=>setName(e.target.value)} className="w-full px-4 py-3 border border-gray-300 rounded-lg" placeholder="Your name" />
          </div>
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">Email</label>
            <input value={email} onChange={e=>setEmail(e.target.value)} className="w-full px-4 py-3 border border-gray-300 rounded-lg" placeholder="you@example.com" />
          </div>
          {accessError && (
            <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg text-sm">
              {accessError}
            </div>
          )}
          <button 
            onClick={handleStart} 
            disabled={!name.trim() || !email.trim() || loadingStatus}
            className="w-full bg-orange-600 text-white py-3 rounded-lg font-semibold hover:bg-orange-700 disabled:bg-gray-300 disabled:cursor-not-allowed"
          >
            {loadingStatus ? 'Starting...' : 'Start'}
          </button>
        </div>
      </div>
    </div>
  );
}


