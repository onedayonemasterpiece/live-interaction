import test from 'node:test';
import assert from 'node:assert/strict';
import {isLiveStopCommand,liveStopConfirmation} from '../browser/live-commands.js';
test('explicit Russian Live stop commands include the reported utterance',()=>{
 for(const text of ['Мира, выключись','Mira, выключись','Мира, пожалуйста, выключись','Пока на этом всё, выключись.','Выключись','Спасибо, отключись, пожалуйста.','Выключи Live','Лайв, остановись!','Заверши разговор','Выключи микрофон','Стоп лайв','Перестань слушать'])assert.equal(isLiveStopCommand(text),true,text);
});
test('negation, quoted examples and lecture edits do not turn the microphone off',()=>{
 for(const text of ['Не выключись','Не выключай Live','Нет, выключись','Напиши в заголовке выключись','Объясни команду выключи микрофон','Скажи слово «выключись»','Выключи слайд','Когда я скажу выключись, закончим','Выключись завтра','Мы пока не заканчиваем'])assert.equal(isLiveStopCommand(text),false,text);
});
test('voice confirmation requires an affirmative utterance and respects negation',()=>{
 for(const text of ['Да.','Да, выключи.','Мира, да, выключи микрофон','Подтверждаю','Да, отключи, пожалуйста'])assert.equal(liveStopConfirmation(text),'confirm',text);
 for(const text of ['Нет.','Да, не выключай','Отмена','Нет, продолжим'])assert.equal(liveStopConfirmation(text),'cancel',text);
 for(const text of ['Напиши да в заголовке','Когда скажу да, выключи','Выключи слайд','Да, заголовок подходит'])assert.equal(liveStopConfirmation(text),null,text);
});
