require('./init.js');
var ds = getDataSource();

describe('evmongodb features', function () {
  var Contact = ds.createModel('contact', {fname: String, mname: String, lname: String});
  var Employee = ds.createModel('employee', {company: String, project: String, contact: Contact});
  before(function(done) {
    Contact.deleteAll();
    Employee.deleteAll();
    Contact.create({fname: 'foo', mname: 'bar', lname: 'baz'}, function(err , contact){
      Employee.create({company: 'ev', project: 'evf', contact: {fname: 'apr', mname: 'hur', lname: 'luz'}}, function(err, employee){
        done();
      });
    });
  });
  
  it('should accept fieldReplace as filter param and return modified results', function (done) {

    Contact.find({fieldReplace: {fname: 'firstname', lname: 'last_name'}, fieldList: ['fname','mname','lname']}, function(err, results){
      results[0].firstname.should.not.be.null;
      results[0].firstname.should.equal('foo');
      results[0].last_name.should.equal('baz');
      results[0].mname.should.not.be.null;
      done();
    });
	  
  });
  
  it('should accept fieldValueReplace as filter param and return modified results', function (done) {

    Contact.find({fieldValueReplace: {fname: {foo: 'FOO'}, lname: {baz : 'BAZ'}}, fieldList: ['fname','mname','lname']}, function(err, results){
      results[0].fname.should.not.be.null;
      results[0].fname.should.equal('FOO');
      results[0].mname.should.not.be.null;
      results[0].lname.should.not.be.null;
      results[0].lname.should.equal('BAZ');
      done();
    });
		  
  });
  
  it('should accept both fieldReplace and fieldValueReplace as filter params and return modified results', function (done) {

    Contact.find({fieldValueReplace: {fname: {foo: 'FOO'}, mname : {bar: 'BAR'}}, fieldReplace: {fname: 'firstname', lname: 'lastname'}, fieldList: ['fname','mname','lname']}, function(err, results){
      results[0].firstname.should.not.be.null;
      results[0].firstname.should.equal('FOO');
      results[0].mname.should.not.be.null;
      results[0].mname.should.equal('BAR');
      results[0].lastname.should.not.be.null;
      results[0].lastname.should.equal('baz');
      done();
    });
    
  });
  
  it('should accept nested key support for both fieldReplace and fieldValueReplace as filter params', function (done) {
    
    Employee.find({fieldValueReplace: {company: {ev: 'Edge Verve'}, project: {evf: 'EV Foundation'}, 'contact.fname': {apr: 'April'}, 'contact.lname': {luz: 'Luzania'}}, fieldReplace: {project: 'Project', 'contact.mname': 'middlename', 'contact.lname': 'lastname'}, fieldList: ['company', 'project', 'contact.fname', 'contact.mname', 'contact.lname']}, function(err, results){
      results[0].company.should.not.be.null;
      results[0].company.should.equal('Edge Verve');
      results[0].Project.should.not.be.null;
      results[0].Project.should.equal('EV Foundation');
      results[0].contact.should.not.be.null;
      results[0].contact.fname.should.not.be.null;
      results[0].contact.fname.should.equal('April');
      results[0].contact.middlename.should.not.be.null;
      results[0].contact.middlename.should.equal('hur');
      results[0].contact.lastname.should.not.be.null;
      results[0].contact.lastname.should.equal('Luzania');      
      done();
    });
    
  });
  
});