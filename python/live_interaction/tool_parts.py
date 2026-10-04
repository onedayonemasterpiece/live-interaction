"""Bounded multimodal tool results, shared by both Python session hosts."""
import base64
import binascii
import copy
import re


class _ToolResult(dict):
    pass


def with_live_tool_parts(result, parts):
    """Attach images to FunctionResponse.parts, never to its JSON text result."""
    if not isinstance(result, dict) or not isinstance(parts, list) or not 1 <= len(parts) <= 2:
        raise ValueError('Invalid Live tool response parts')
    for part in parts:
        blob = part.get('inlineData', {}) if isinstance(part, dict) else {}
        data, name = blob.get('data'), blob.get('displayName')
        if (blob.get('mimeType') not in {'image/jpeg', 'image/png', 'image/webp'}
                or not isinstance(data, str) or len(data) > 700000
                or not isinstance(name, str) or not re.fullmatch(r'[a-zA-Z0-9_.-]{1,80}', name)):
            raise ValueError('Invalid Live image response part')
        try:
            if len(base64.b64decode(data, validate=True)) > 512 * 1024:
                raise ValueError('Live image response part exceeds limit')
        except (binascii.Error, UnicodeError) as exc:
            raise ValueError('Invalid Live image response part') from exc
    wrapped = _ToolResult(result)
    wrapped.parts = copy.deepcopy(parts)
    return wrapped


def function_response(name, call_id, result):
    response = {'name': name, 'id': call_id, 'response': {'result': result}}
    if isinstance(result, _ToolResult):
        response.update(response=dict(result), parts=result.parts)
    return response
