function buildUrl(path, params = {}) {
  const base = window.location.origin && window.location.origin !== 'null'
    ? window.location.origin
    : 'http://127.0.0.1:8501'
  const url = new URL(path, base)
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, value)
    }
  })
  return url
}

async function parseResponse(response) {
  const contentType = response.headers.get('content-type') || ''
  if (contentType.includes('application/json')) {
    return response.json()
  }
  const text = await response.text()
  return { error: text || `Request failed: ${response.status}` }
}

export async function apiGet(path, params = {}) {
  const url = buildUrl(path, params)
  let response
  try {
    response = await fetch(url)
  } catch (error) {
    throw new Error('无法连接到本地仪表盘服务，请确认 http://127.0.0.1:8501 已启动。')
  }
  const payload = await parseResponse(response)
  if (!response.ok) {
    throw new Error(payload.error || `Request failed: ${response.status}`)
  }
  return payload
}

export async function apiPost(path, body = {}) {
  const url = buildUrl(path)
  let response
  try {
    response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    })
  } catch (error) {
    throw new Error('无法连接到本地仪表盘服务，请确认 http://127.0.0.1:8501 已启动。')
  }
  const payload = await parseResponse(response)
  if (!response.ok) {
    throw new Error(payload.error || `Request failed: ${response.status}`)
  }
  return payload
}
