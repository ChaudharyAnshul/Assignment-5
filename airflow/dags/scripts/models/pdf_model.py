from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import datetime
import re


class PDFDataModel(BaseModel):
  curriculum_topic: Optional[str]
  curriculum_refresher_reading:  Optional[str]
  curriculum_year: Optional[int] = Field(default = None)
  cfa_level:  Optional[int]
  learning_outcomes: Optional[str] = Field(default = None)

  @field_validator("curriculum_topic")
  @classmethod
  def topic_validator(cls, v):
    ''' check spaces in topic '''
    if v:
      if not Validate_topic_test_rr(v):
        raise ValueError("Test RR page")
      if not validate_string_spaces(v):
        raise ValueError('Unwanted spaces in the string')
      return v.title()
    
  @field_validator("curriculum_year")
  @classmethod
  def year_validator(cls, v):
    ''' check year on the content '''
    if v:
      reg_pattern=r'^20\d{2}$'
      if not re.match(reg_pattern, str(v)):
        raise ValueError('Year not in range')
      if v > datetime.now().year:
        raise ValueError('Year is in Future')
      return v
    
  @field_validator("cfa_level")
  @classmethod
  def level_validator(cls, v):
    ''' check level digits '''
    if v:
      reg_pattern=r'^[123]$'
      if not re.match(reg_pattern, str(v)):
        raise ValueError('Level not in range')
      return v
    
  @field_validator("learning_outcomes", "curriculum_refresher_reading")
  @classmethod
  def introduction_validator(cls, v):
    ''' check all the text paragraphs for spaces and unwanted characters '''
    if v:
      if not validate_string_spaces(v):
        raise ValueError('Unwanted spaces in the string')
      if not Validate_string_line_space_char(v):
        raise ValueError('Unwanted line space character') 
      return v
    
    
# helper function to validate spaces in string 
def validate_string_spaces(v):
  if v:
    start = v.startswith(" ")
    end = v.endswith(" ")
    if start or end:
      return False
    if re.findall(r'\s{2,}', v):
      return False
  return True

def Validate_string_line_space_char(v):
  if v:
    if '\n' in v:
      return False
    if '□' in v:
      return False
    return True
  
def Validate_topic_test_rr(v):
  if v:
    if "test rr" in v.lower():
      return False
    return True