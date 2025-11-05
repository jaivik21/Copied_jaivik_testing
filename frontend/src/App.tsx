import { useEffect, useState } from 'react';
import CreateInterview from './components/CreateInterview';
import UploadCV from './components/UploadCV';
import InterviewSession from './components/InterviewSession';
import InterviewDetail from './components/InterviewDetail';
import CandidateAnalytics from './components/CandidateAnalytics';
import CandidateStart from './components/CandidateStart';
import InterviewsList from './components/InterviewsList';
import InterviewersList from './components/InterviewersList';
import CreateInterviewer from './components/CreateInterviewer';

type Step = 'create' | 'upload' | 'interview';

function App() {
  const [currentStep, setCurrentStep] = useState<Step>('create');
  const [interviewId, setInterviewId] = useState<string>('');
  const [candidateName, setCandidateName] = useState<string>('');
  const [candidateEmail, setCandidateEmail] = useState<string>('');

  const apiBaseUrl = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

  const handleInterviewCreated = (id: string) => {
    setInterviewId(id);
    setCurrentStep('upload');
  };

  const handleCVUploaded = (name: string, email: string) => {
    setCandidateName(name);
    setCandidateEmail(email);
    setCurrentStep('interview');
  };

  // Simple path-based routing: /interviews/:id or /interviews/:id/responses/:responseId
  const [path, setPath] = useState(window.location.pathname);
  useEffect(() => {
    const fn = () => setPath(window.location.pathname);
    window.addEventListener('popstate', fn);
    return () => window.removeEventListener('popstate', fn);
  }, []);

  const parts = (path || '').split('/').filter(Boolean);
  if (parts[0] === 'candidate' && parts[1] === 'interview' && parts[2]) {
    const iid = parts[2];
    return <CandidateStart apiBaseUrl={apiBaseUrl} interviewId={iid} />
  }
  if (parts[0] === 'interviews' && parts[1]) {
    const iid = parts[1];
    if (parts[2] === 'responses' && parts[3]) {
      return <CandidateAnalytics apiBaseUrl={apiBaseUrl} interviewId={iid} responseId={parts[3]} />
    }
    return <InterviewDetail apiBaseUrl={apiBaseUrl} interviewId={iid} />
  }

  if (parts[0] === 'interviewers') {
    if (parts[1] === 'create') {
      return <CreateInterviewer apiBaseUrl={apiBaseUrl} />
    }
    if (parts[1] && parts[1] !== 'create') {
      // Edit mode
      return <CreateInterviewer apiBaseUrl={apiBaseUrl} interviewerId={parts[1]} />
    }
    return <InterviewersList apiBaseUrl={apiBaseUrl} />
  }

  if (parts[0] === 'create') {
    const onInterviewCreated = (id: string) => {
      window.history.pushState({}, '', `/interviews/${id}`);
      window.dispatchEvent(new PopStateEvent('popstate'));
    };
    return <CreateInterview onInterviewCreated={onInterviewCreated} apiBaseUrl={apiBaseUrl} />
  }

  // Default: interviews grid
  return <InterviewsList apiBaseUrl={apiBaseUrl} />
}

export default App;
