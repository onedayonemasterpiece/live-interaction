// Only explicit session-control utterances, never keywords inside lecture/edit requests.
export function isLiveStopCommand(text){
  let value=String(text??'').toLowerCase().replaceAll('ё','е').replace(/[.,!?;:—–-]+/g,' ').replace(/\s+/g,' ').trim();
  if(!value||value.length>240||/(?:^|\s)(?:не|нет)(?:\s|$)/u.test(value))return false;
  const prefix=/^(?:(?:пока на этом все|на этом все|пока все|спасибо|пожалуйста|хорошо|ладно|теперь|все|мира|mira|gemini|джемини|лайв|live)\s+)+/u;
  value=value.replace(prefix,'').replace(/\s+(?:пожалуйста|спасибо|на этом все|пока)$/u,'').trim();
  return /^(?:(?:выключись|отключись|остановись)(?:\s+(?:сейчас|пожалуйста))?|(?:выключи|отключи|останови|заверши|закрой)\s+(?:live|лайв|голосовой режим|голосовой чат|разговор|сеанс|сессию|микрофон|прослушивание)|стоп(?:\s+(?:live|лайв))?|хватит слушать|перестань слушать)$/u.test(value);
}

// A confirmation is an entire utterance, never a keyword inside an edit request.
export function liveStopConfirmation(text){
  const value=String(text??'').toLowerCase().replaceAll('ё','е').replace(/[.,!?;:—–-]+/g,' ').replace(/\s+/g,' ').trim();
  if(/(?:^|\s)(?:не|нет|отмена|отмени)(?:\s|$)/u.test(value))return 'cancel';
  if(/^(?:(?:мира|mira)\s+)?(?:да(?:\s+(?:выключи|выключай|отключи|отключай)(?:\s+микрофон)?)?|подтверждаю)(?:\s+пожалуйста)?$/u.test(value))return 'confirm';
  return null;
}
