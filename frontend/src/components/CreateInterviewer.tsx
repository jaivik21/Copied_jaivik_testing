import { useState, useEffect } from 'react';
import { ArrowLeft, Save } from 'lucide-react';

interface Props { 
  apiBaseUrl: string;
  interviewerId?: string;
}

export default function CreateInterviewer({ apiBaseUrl, interviewerId }: Props) {
  const [formData, setFormData] = useState({
    name: '',
    persona: '',
    accent: '',
    elevenlabs_voice_id: '',
    avatar_url: '',
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [isEditMode, setIsEditMode] = useState(false);

  useEffect(() => {
    if (interviewerId) {
      setIsEditMode(true);
      loadInterviewer();
    }
  }, [interviewerId, apiBaseUrl]);

  const loadInterviewer = async () => {
    try {
      const res = await fetch(`${apiBaseUrl}/api/interviewer/get-interviewer/${interviewerId}`).then(r => r.json());
      if (res) {
        setFormData({
          name: res.name || '',
          persona: res.persona || '',
          accent: res.accent || '',
          elevenlabs_voice_id: res.elevenlabs_voice_id || '',
          avatar_url: res.avatar_url || '',
        });
      }
    } catch (e: any) {
      setError(e?.message || 'Failed to load interviewer');
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError('');

    try {
      const fd = new FormData();
      fd.append('name', formData.name);
      if (formData.persona) fd.append('persona', formData.persona);
      if (formData.accent) fd.append('accent', formData.accent);
      if (formData.elevenlabs_voice_id) fd.append('elevenlabs_voice_id', formData.elevenlabs_voice_id);
      if (formData.avatar_url) fd.append('avatar_url', formData.avatar_url);

      let response;
      if (isEditMode && interviewerId) {
        fd.append('interviewer_id', interviewerId);
        response = await fetch(`${apiBaseUrl}/api/interviewer/update-interviewer`, {
          method: 'POST',
          body: fd,
        });
      } else {
        response = await fetch(`${apiBaseUrl}/api/interviewer/create-interviewer`, {
          method: 'POST',
          body: fd,
        });
      }

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText || 'Failed to save interviewer');
      }

      // Redirect to interviewers list
      window.history.pushState({}, '', '/interviewers');
      window.dispatchEvent(new PopStateEvent('popstate'));
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  const goBack = () => {
    window.history.pushState({}, '', '/interviewers');
    window.dispatchEvent(new PopStateEvent('popstate'));
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-50 flex items-center justify-center p-6">
      <div className="bg-white rounded-2xl shadow-xl max-w-2xl w-full p-8">
        <div className="flex items-center gap-3 mb-8">
          <button
            onClick={goBack}
            className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
            title="Go back"
          >
            <ArrowLeft className="w-5 h-5" />
          </button>
          <div>
            <h1 className="text-3xl font-bold text-gray-900">
              {isEditMode ? 'Edit Interviewer' : 'Create Interviewer'}
            </h1>
            <p className="text-gray-600">Configure interviewer profile and voice settings</p>
          </div>
        </div>

        {error && (
          <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-6">
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">
              Interviewer Name <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              required
              value={formData.name}
              onChange={(e) => setFormData({ ...formData, name: e.target.value })}
              className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="e.g., Explorer Lina"
            />
          </div>

          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">Persona</label>
            <input
              type="text"
              value={formData.persona}
              onChange={(e) => setFormData({ ...formData, persona: e.target.value })}
              className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="e.g., Explorer, Empathetic, Professional"
            />
            <p className="text-xs text-gray-500 mt-1">A descriptive personality type for this interviewer</p>
          </div>

          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">Accent</label>
            <select
              value={formData.accent}
              onChange={(e) => setFormData({ ...formData, accent: e.target.value })}
              className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              <option value="">Select an accent</option>
              <option value="American">American</option>
              <option value="British">British</option>
              <option value="Australian">Australian</option>
              <option value="Canadian">Canadian</option>
              <option value="Irish">Irish</option>
              <option value="Indian">Indian</option>
              <option value="Spanish">Spanish</option>
              <option value="French">French</option>
              <option value="German">German</option>
              <option value="Italian">Italian</option>
            </select>
          </div>

          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">
              ElevenLabs Voice ID
            </label>
            <input
              type="text"
              value={formData.elevenlabs_voice_id}
              onChange={(e) => setFormData({ ...formData, elevenlabs_voice_id: e.target.value })}
              className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="e.g., pNInz6obpgDQGcFmaJgB"
            />
            <p className="text-xs text-gray-500 mt-1">
              Voice ID from your ElevenLabs account. You can find this in your ElevenLabs dashboard.
            </p>
          </div>

          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-2">Avatar URL</label>
            <input
              type="url"
              value={formData.avatar_url}
              onChange={(e) => setFormData({ ...formData, avatar_url: e.target.value })}
              className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              placeholder="https://example.com/avatar.png"
            />
            <p className="text-xs text-gray-500 mt-1">
              URL to the interviewer's avatar image
            </p>
          </div>

          <div className="flex gap-3">
            <button
              type="button"
              onClick={goBack}
              className="flex-1 px-6 py-3 border border-gray-300 rounded-lg font-semibold hover:bg-gray-50 transition"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={loading}
              className="flex-1 px-6 py-3 bg-blue-600 text-white rounded-lg font-semibold hover:bg-blue-700 transition disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
            >
              <Save className="w-4 h-4" />
              {loading ? 'Saving...' : isEditMode ? 'Update Interviewer' : 'Create Interviewer'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

